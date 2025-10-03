from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
import subprocess, tempfile, glob, os, re, shutil

app = FastAPI()

# ----------------------
# Request schema
# ----------------------
class TranscriptRequest(BaseModel):
    url: str

# ----------------------
# Auth helper
# ----------------------
def ok_api_key(x_api_key: str):
    if x_api_key != "ryttr_super_secret_456_even_longer":
        raise HTTPException(status_code=401, detail="Invalid API key")

# ----------------------
# Your NuclearDuplicateRemoval (shortened to reuse your Colab logic)
# ----------------------
class NuclearDuplicateRemoval:
    def _parse_srt(self, content):
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
            if not m: continue
            start, end = m.group(1), m.group(2)
            text = ' '.join(lines[text_start_idx:]).strip()
            if text:
                segments.append({"start": start, "end": end, "text": text})
        return segments

    def _normalize(self, text):
        t = text.lower().strip()
        t = re.sub(r'[^\w\s]', '', t)
        t = re.sub(r'\s+', ' ', t)
        return t

    def _nuclear_duplicate_removal(self, segments):
        seen = set(); out = []
        for s in segments:
            norm = self._normalize(s["text"])
            if norm not in seen:
                out.append(s); seen.add(norm)
        return out

# ----------------------
# Route 1: basic transcripts (youtube-transcript-api style)
# ----------------------
@app.post("/transcripts")
def transcripts(req: TranscriptRequest, x_api_key: str = Header(None)):
    ok_api_key(x_api_key)
    # This is still your placeholder logic
    return {"ok": True, "message": "Default transcripts endpoint (limited)"}

# ----------------------
# Route 2: yt-dlp extractor (like Colab)
# ----------------------
@app.post("/transcripts/yt")
def transcripts_with_ytdlp(req: TranscriptRequest, x_api_key: str = Header(None)):
    ok_api_key(x_api_key)
    url = str(req.url)

    with tempfile.TemporaryDirectory() as tmp:
        os.chdir(tmp)
        cmd = [
            "yt-dlp", "--skip-download",
            "--write-auto-sub", "--sub-langs", "en",
            "--convert-subs", "srt",
            url
        ]
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

        srt_files = glob.glob("*.srt")
        if not srt_files:
            raise HTTPException(status_code=404, detail="No captions (even auto) available")

        path = srt_files[0]
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

        nuke = NuclearDuplicateRemoval()
        segments = nuke._parse_srt(content)
        clean_segments = nuke._nuclear_duplicate_removal(segments)

        return {
            "ok": True,
            "segment_count": len(clean_segments),
            "segments": clean_segments[:20],  # sample only
            "full_text": " ".join([s["text"] for s in clean_segments]),
            "stdout_tail": p.stdout[-500:],
            "stderr_tail": p.stderr[-500:]
        }
