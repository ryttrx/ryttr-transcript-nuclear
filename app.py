import os
import re
import glob
import shutil
import tempfile
import subprocess
from typing import Dict, Any, List

from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, HttpUrl

# New
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    TranscriptsDisabled,
    NoTranscriptFound,
    VideoUnavailable,
)

PY_SERVICE_KEY = os.environ.get("PY_SERVICE_KEY", "")
CORS_ORIGINS = [o.strip() for o in os.environ.get("CORS_ORIGINS", "").split(",") if o.strip()]

app = FastAPI(title="ryttr transcript nuclear - robust")

if CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ORIGINS,
        allow_credentials=False,
        allow_methods=["POST", "OPTIONS", "GET"],
        allow_headers=["*"],
    )

class Req(BaseModel):
    url: HttpUrl

class Resp(BaseModel):
    ok: bool
    segments: List[Dict[str, Any]] | None = None
    segment_count: int | None = None
    full_text: str | None = None
    video_title: str | None = None
    youtube_url: str | None = None
    lang: str | None = None
    message: str | None = None

yt_id_regex = re.compile(r"(?:v=|youtu\.be/)([A-Za-z0-9_-]{11})")

def extract_video_id(url: str) -> str | None:
    m = yt_id_regex.search(url)
    return m.group(1) if m else None

# ---------- Duplicate removal helpers (from your Colab logic) ----------
def normalize(text: str) -> str:
    t = text.lower().strip()
    t = re.sub(r"[^\w\s]", "", t)
    t = re.sub(r"\s+", " ", t)
    return t

def jaccard(a: str, b: str) -> float:
    s1, s2 = set(a.split()), set(b.split())
    if not s1 or not s2:
        return 0.0
    return len(s1 & s2) / len(s1 | s2)

def timestamp_to_seconds(ts: str) -> float:
    ts = ts.replace(",", ".")
    try:
        parts = ts.split(".")[0].split(":")
        if len(parts) == 2:
            h, m, s = 0, int(parts[0]), int(parts[1])
        else:
            h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
        millis = int(ts.split(".")[-1]) if "." in ts else 0
        return h * 3600 + m * 60 + s + millis / 1000.0
    except Exception:
        return 0.0

def parse_srt(content: str) -> List[Dict[str, Any]]:
    segments = []
    blocks = re.split(r"\n\s*\n", content.strip())
    for block in blocks:
        lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
        if len(lines) < 2:
            continue
        ts_line = None
        text_start_idx = 1
        if lines[0].isdigit() and len(lines) >= 3 and re.search(r"-->", lines[1]):
            ts_line = lines[1]; text_start_idx = 2
        elif re.search(r"-->", lines[0]):
            ts_line = lines[0]; text_start_idx = 1
        else:
            continue
        m = re.search(r"(\d{2}:\d{2}:\d{2}[,.]\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2}[,.]\d{3})", ts_line)
        if not m:
            continue
        start, end = m.group(1), m.group(2)
        text = " ".join(lines[text_start_idx:]).strip()
        if text:
            segments.append({
                "start": start.replace(".", ","),
                "end": end.replace(".", ","),
                "text": text,
                "start_seconds": timestamp_to_seconds(start),
            })
    segments.sort(key=lambda x: x["start_seconds"])
    return segments

def parse_vtt(content: str) -> List[Dict[str, Any]]:
    content = re.sub(r"^\ufeff?", "", content)
    content = re.sub(r"^WEBVTT.*\n", "", content, flags=re.IGNORECASE)
    segments = []
    blocks = re.split(r"\n\s*\n", content.strip())
    for block in blocks:
        lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
        if not lines:
            continue
        ts_idx = 0 if "-->" in lines[0] else 1 if len(lines) > 1 and "-->" in lines[1] else -1
        if ts_idx == -1:
            continue
        ts_line = lines[ts_idx]
        m = re.search(r"(\d{2}:\d{2}:\d{2}[.,]\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2}[.,]\d{3})", ts_line)
        if not m:
            m2 = re.search(r"(\d{2}:\d{2}[.,]\d{3})\s*-->\s*(\d{2}:\d{2}[.,]\d{3})", ts_line)
            if m2:
                start = "00:" + m2.group(1); end = "00:" + m2.group(2)
            else:
                continue
        else:
            start, end = m.group(1), m.group(2)
        text = " ".join(lines[ts_idx + 1:]).strip()
        text = re.sub(r"<[^>]+>", "", text)
        if text:
            segments.append({
                "start": start.replace(".", ","),
                "end": end.replace(".", ","),
                "text": text,
                "start_seconds": timestamp_to_seconds(start),
            })
    segments.sort(key=lambda x: x["start_seconds"])
    return segments

def nuclear_cleanup(segments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not segments:
        return []
    # exact
    seen = set(); out = []
    for s in segments:
        n = normalize(s["text"])
        if n not in seen:
            out.append(s); seen.add(n)
    segments = out
    # similar adjacent
    if len(segments) > 1:
        out = [segments[0]]
        for i in range(1, len(segments)):
            a = normalize(segments[i]["text"])
            b = normalize(segments[i-1]["text"])
            if jaccard(a, b) < 0.7:
                out.append(segments[i])
        segments = out
    # time near-duplicates
    if len(segments) > 1:
        out = [segments[0]]
        for i in range(1, len(segments)):
            cur, prev = segments[i], segments[i-1]
            td = cur["start_seconds"] - prev["start_seconds"]
            if td > 5 or normalize(cur["text"]) != normalize(prev["text"]):
                out.append(cur)
        segments = out
    # partial includes
    norms = [normalize(s["text"]) for s in segments]
    out = []
    for i, s in enumerate(segments):
        a = norms[i]; keep = True
        for j, b in enumerate(norms):
            if i == j: continue
            if a in b and len(a) <= len(b):
                keep = False; break
        if keep: out.append(s)
    return out

# ---------- yt-dlp helpers ----------
def run_ytdlp_for_subs(youtube_url: str, auto: bool) -> str | None:
    """
    Try to download subtitles in any language.
    Returns path to .srt or .vtt file, or None.
    """
    with tempfile.TemporaryDirectory() as tmp:
        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            sub_langs = "all"
            convert_flag = ["--convert-subs", "srt"] if shutil.which("ffmpeg") else []
            cmd = [
                "yt-dlp", "--skip-download",
                "--write-auto-sub" if auto else "--write-sub",
                "--sub-langs", sub_langs,
                *convert_flag, "--no-warnings",
                youtube_url,
            ]
            subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            # Prefer srt, else vtt
            srt = glob.glob("*.srt")
            if srt:
                return _copy_to_cwd(srt[0], cwd)
            vtt = glob.glob("*.vtt")
            if vtt:
                return _copy_to_cwd(vtt[0], cwd)
            return None
        finally:
            os.chdir(cwd)

def _copy_to_cwd(path_in_tmp: str, cwd: str) -> str:
    name = os.path.basename(path_in_tmp)
    dest = os.path.join(cwd, name)
    shutil.copy(path_in_tmp, dest)
    return dest

def parse_sub_file(path: str) -> List[Dict[str, Any]]:
    ext = os.path.splitext(path)[1].lower()
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()
    if ext == ".srt":
        return parse_srt(content)
    if ext == ".vtt":
        return parse_vtt(content)
    return []

# ---------- youtube-transcript-api helpers ----------
def yta_best_effort(video_id: str) -> tuple[list[dict], str] | None:
    """
    Try English. If not available, try generated English.
    If still not, try translate to English.
    If still not, return first available language.
    Returns (segments, language) or None.
    """
    try:
        transcripts = YouTubeTranscriptApi.list_transcripts(video_id)

        # 1) direct English
        for code in ["en", "en-US", "en-GB"]:
            try:
                t = transcripts.find_transcript([code])
                data = t.fetch()
                return (_yta_to_segments(data), t.language_code or "en")
            except Exception:
                pass

        # 2) auto-generated English
        try:
            t = transcripts.find_generated_transcript(["en"])
            data = t.fetch()
            return (_yta_to_segments(data), t.language_code or "en")
        except Exception:
            pass

        # 3) translate to English if possible
        try:
            # pick any transcript and translate
            any_t = next(iter(transcripts))
            translated = any_t.translate("en")
            data = translated.fetch()
            return (_yta_to_segments(data), "en")
        except Exception:
            pass

        # 4) fallback to first available language as-is
        try:
            any_t = next(iter(transcripts))
            data = any_t.fetch()
            return (_yta_to_segments(data), any_t.language_code or any_t.language or "unknown")
        except Exception:
            pass

        return None
    except (TranscriptsDisabled, NoTranscriptFound, VideoUnavailable):
        return None
    except Exception:
        return None

def _yta_to_segments(chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for c in chunks:
        start = float(c.get("start", 0.0))
        dur = float(c.get("duration", 0.0))
        end = start + dur
        def fmt(sec: float) -> str:
            h = int(sec // 3600)
            m = int((sec % 3600) // 60)
            s = int(sec % 60)
            ms = int((sec - int(sec)) * 1000)
            return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"
        out.append({
            "start": fmt(start),
            "end": fmt(end),
            "text": c.get("text", ""),
            "start_seconds": start,
        })
    out.sort(key=lambda x: x["start_seconds"])
    return out

# ---------- API ----------
@app.get("/")
def root():
    return {"ok": True, "service": "ryttr transcript nuclear - robust"}

@app.post("/transcript", response_model=Resp)
def transcript(req: Req, x_api_key: str = Header(None)):
    if not PY_SERVICE_KEY or x_api_key != PY_SERVICE_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    url = str(req.url)
    vid = extract_video_id(url)
    if not vid:
        raise HTTPException(status_code=400, detail="Invalid YouTube URL")

    # Try 1 - manual subs in any language via yt-dlp
    path = run_ytdlp_for_subs(url, auto=False)
    segments = parse_sub_file(path) if path else []
    method = "yt-dlp manual any-lang"

    # Try 2 - auto subs in any language via yt-dlp
    if not segments:
        path = run_ytdlp_for_subs(url, auto=True)
        segments = parse_sub_file(path) if path else []
        method = "yt-dlp auto any-lang"

    # Try 3 - youtube-transcript-api best effort
    lang = None
    if not segments:
        yta = yta_best_effort(vid)
        if yta:
            segments, lang = yta
            method = "youtube-transcript-api best-effort"

    if not segments:
        raise HTTPException(status_code=404, detail="No transcript available for this video")

    # Cleanup duplicates
    clean = nuclear_cleanup(segments)
    full_text = " ".join([s["text"] for s in clean])

    return {
        "ok": True,
        "segments": clean,
        "segment_count": len(clean),
        "full_text": full_text,
        "video_title": None,  # optional to look up with yt-dlp --get-title if you want
        "youtube_url": url,
        "lang": lang or "unknown",
        "message": f"method={method}",
    }
