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

# -------- Config from environment --------
PY_SERVICE_KEY = os.environ.get("PY_SERVICE_KEY", "")
CORS_ORIGINS = [o.strip() for o in os.environ.get("CORS_ORIGINS", "").split(",") if o.strip()]

app = FastAPI(title="ryttr transcript nuclear")

if CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ORIGINS,
        allow_credentials=False,
        allow_methods=["POST", "OPTIONS", "GET"],
        allow_headers=["*"],
    )

yt_id_regex = re.compile(r"(?:v=|youtu\.be/)([A-Za-z0-9_-]{11})")

class Req(BaseModel):
    url: HttpUrl

class Resp(BaseModel):
    ok: bool
    segments: List[Dict[str, Any]] | None = None
    segment_count: int | None = None
    full_text: str | None = None
    video_title: str | None = None
    youtube_url: str | None = None
    message: str | None = None

# -------- Your Colab logic adapted to a class --------
class NuclearDuplicateRemoval:
    def get_video_title(self, youtube_url: str) -> str:
        try:
            cmd = ['yt-dlp', '--get-title', '--no-warnings', youtube_url]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0 and result.stdout.strip():
                title = result.stdout.strip()
                clean_title = re.sub(r'[<>:"/\\|?*]', '', title)
                clean_title = re.sub(r'[^\w\s-]', '', clean_title)
                clean_title = clean_title.replace(' ', '_')[:50]
                return clean_title or "youtube_transcript"
        except Exception:
            pass
        return "youtube_transcript"

    def extract_transcript(self, youtube_url: str):
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                original_dir = os.getcwd()
                os.chdir(temp_dir)

                video_title = self.get_video_title(youtube_url)

                sub_path = self._download_single_track(youtube_url)
                if not sub_path:
                    os.chdir(original_dir)
                    return {"error": "No captions found. Try a different video or language."}

                segments = self._process_subs(sub_path)
                if not segments:
                    os.chdir(original_dir)
                    return {"error": "Could not process captions"}

                clean_segments = self._nuclear_duplicate_removal(segments)
                os.chdir(original_dir)

                return {
                    "success": True,
                    "segments": clean_segments,
                    "segment_count": len(clean_segments),
                    "full_text": " ".join([s["text"] for s in clean_segments]),
                    "video_title": video_title,
                    "youtube_url": youtube_url
                }
        except Exception as e:
            return {"error": f"Processing error: {str(e)}"}

    def _download_single_track(self, youtube_url: str):
        sub_langs = "en.*,en"
        convert_flag = ["--convert-subs", "srt"] if shutil.which("ffmpeg") else []

        # Try manual subs first
        cmd_manual = [
            "yt-dlp", "--skip-download",
            "--write-sub", "--sub-langs", sub_langs,
            *convert_flag, "--no-warnings", youtube_url
        ]
        subprocess.run(cmd_manual, capture_output=True, text=True, timeout=300)
        srt = glob.glob("*.srt")
        if srt:
            return srt[0]
        vtt = glob.glob("*.vtt")
        if vtt:
            return vtt[0]

        # Fallback to auto-generated
        cmd_auto = [
            "yt-dlp", "--skip-download",
            "--write-auto-sub", "--sub-langs", sub_langs,
            *convert_flag, "--no-warnings", youtube_url
        ]
        subprocess.run(cmd_auto, capture_output=True, text=True, timeout=300)
        srt = glob.glob("*.srt")
        if srt:
            return srt[0]
        vtt = glob.glob("*.vtt")
        if vtt:
            return vtt[0]

        return None

    def _process_subs(self, path: str):
        ext = os.path.splitext(path)[1].lower()
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
        if ext == ".srt":
            return self._parse_srt(content)
        elif ext == ".vtt":
            return self._parse_vtt(content)
        return []

    def _parse_srt(self, content: str):
        segments = []
        blocks = re.split(r'\n\s*\n', content.strip())
        for block in blocks:
            lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
            if len(lines) < 2:
                continue
            ts_line = None
            text_start_idx = 1
            if lines[0].isdigit() and len(lines) >= 3 and re.search(r'-->', lines[1]):
                ts_line = lines[1]; text_start_idx = 2
            elif re.search(r'-->', lines[0]):
                ts_line = lines[0]; text_start_idx = 1
            else:
                continue
            m = re.search(r'(\d{2}:\d{2}:\d{2}[,.]\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2}[,.]\d{3})', ts_line)
            if not m:
                continue
            start, end = m.group(1), m.group(2)
            text = ' '.join(lines[text_start_idx:]).strip()
            if text:
                segments.append({
                    "start": start.replace('.', ','),
                    "end": end.replace('.', ','),
                    "text": text,
                    "start_seconds": self._timestamp_to_seconds(start)
                })
        segments.sort(key=lambda x: x["start_seconds"])
        return segments

    def _parse_vtt(self, content: str):
        content = re.sub(r'^\ufeff?', '', content)
        content = re.sub(r'^WEBVTT.*\n', '', content, flags=re.IGNORECASE)
        segments = []
        blocks = re.split(r'\n\s*\n', content.strip())
        for block in blocks:
            lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
            if not lines:
                continue
            ts_idx = 0 if "-->" in lines[0] else 1 if len(lines) > 1 and "-->" in lines[1] else -1
            if ts_idx == -1:
                continue
            ts_line = lines[ts_idx]
            m = re.search(r'(\d{2}:\d{2}:\d{2}[.,]\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2}[.,]\d{3})', ts_line)
            if not m:
                m2 = re.search(r'(\d{2}:\d{2}[.,]\d{3})\s*-->\s*(\d{2}:\d{2}[.,]\d{3})', ts_line)
                if m2:
                    start = "00:" + m2.group(1); end = "00:" + m2.group(2)
                else:
                    continue
            else:
                start, end = m.group(1), m.group(2)
            text = ' '.join(lines[ts_idx+1:]).strip()
            text = re.sub(r'<[^>]+>', '', text)
            if text:
                segments.append({
                    "start": start.replace('.', ','),
                    "end": end.replace('.', ','),
                    "text": text,
                    "start_seconds": self._timestamp_to_seconds(start)
                })
        segments.sort(key=lambda x: x["start_seconds"])
        return segments

    def _nuclear_duplicate_removal(self, segments: List[Dict[str, Any]]):
        if not segments:
            return []
        segments = self._remove_exact_duplicates(segments)
        segments = self._remove_similar_duplicates(segments)
        segments = self._remove_time_duplicates(segments)
        segments = self._remove_partial_duplicates(segments)
        return segments

    def _remove_exact_duplicates(self, segments):
        seen = set(); out = []
        for s in segments:
            norm = self._normalize(s["text"])
            if norm not in seen:
                out.append(s); seen.add(norm)
        return out

    def _remove_similar_duplicates(self, segments):
        if len(segments) <= 1: return segments
        out = [segments[0]]
        for i in range(1, len(segments)):
            curr = self._normalize(segments[i]["text"])
            prev = self._normalize(segments[i-1]["text"])
            if self._jaccard(curr, prev) < 0.7:
                out.append(segments[i])
        return out

    def _remove_time_duplicates(self, segments):
        if len(segments) <= 1: return segments
        out = [segments[0]]
        for i in range(1, len(segments)):
            curr, prev = segments[i], segments[i-1]
            td = curr["start_seconds"] - prev["start_seconds"]
            if td > 5 or self._normalize(curr["text"]) != self._normalize(prev["text"]):
                out.append(curr)
        return out

    def _remove_partial_duplicates(self, segments):
        out = []; norms = [self._normalize(s["text"]) for s in segments]
        for i, s in enumerate(segments):
            keep = True; a = norms[i]
            for j, b in enumerate(norms):
                if i == j: continue
                if a in b and len(a) <= len(b):
                    keep = False; break
            if keep: out.append(s)
        return out

    def _normalize(self, text: str):
        t = text.lower().strip()
        t = re.sub(r'[^\w\s]', '', t)
        t = re.sub(r'\s+', ' ', t)
        return t

    def _jaccard(self, a: str, b: str):
        s1, s2 = set(a.split()), set(b.split())
        if not s1 or not s2: return 0.0
        return len(s1 & s2) / len(s1 | s2)

    def _timestamp_to_seconds(self, ts: str):
        ts = ts.replace(',', '.')
        try:
            parts = ts.split('.')[0].split(':')
            if len(parts) == 2: h, m, s = 0, int(parts[0]), int(parts[1])
            else: h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
            millis = int(ts.split('.')[-1]) if '.' in ts else 0
            return h*3600 + m*60 + s + millis/1000.0
        except Exception:
            return 0.0

# -------- FastAPI endpoints --------
@app.get("/")
def root():
    return {"ok": True, "service": "ryttr transcript nuclear"}

@app.post("/transcript", response_model=Resp)
def transcript(req: Req, x_api_key: str = Header(None)):
    if not PY_SERVICE_KEY or x_api_key != PY_SERVICE_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    url = str(req.url)
    if "youtube.com" not in url and "youtu.be" not in url:
        raise HTTPException(status_code=400, detail="Invalid YouTube URL")

    tool = NuclearDuplicateRemoval()
    result = tool.extract_transcript(url)
    if "error" in result:
        raise HTTPException(status_code=422, detail=result["error"])

    return {
        "ok": True,
        "segments": result["segments"],
        "segment_count": result["segment_count"],
        "full_text": result["full_text"],
        "video_title": result["video_title"],
        "youtube_url": result["youtube_url"],
    }
