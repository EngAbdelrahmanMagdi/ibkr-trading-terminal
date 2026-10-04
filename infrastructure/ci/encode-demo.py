"""Encode one chronological browser recording without modifying its content."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys


def native_path(path: Path, binary: Path) -> str:
    if sys.platform == "cygwin" and binary.suffix == ".exe":
        return subprocess.check_output(["cygpath", "-am", str(path)], text=True).strip()
    return str(path.resolve())


def encode(source: Path, output: Path, binary: Path) -> None:
    raw = source.parent.parent
    if json.loads((raw / ".last-run.json").read_text()).get("status") != "passed":
        raise ValueError("Only a successful real browser scenario may be encoded")
    evidence = json.loads((raw / "demo-evidence.json").read_text())
    if evidence["executions"] != 2 or evidence["closedPosition"] is not True or evidence["portfolio"]["dayPnl"]["available"] is not True:
        raise ValueError("Missing authoritative round-trip evidence")
    version = subprocess.check_output([str(binary), "-version"], text=True).splitlines()[0]
    if not version.startswith("ffmpeg version 9.0.2"):
        raise ValueError("Use the verified FFmpeg 9.0.2 encoder")
    output.mkdir(parents=True, exist_ok=True)
    video = output / "terminal.mp4"
    subprocess.run([str(binary), "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
                    "-i", native_path(source, binary), "-an", "-c:v", "libx264",
                    "-preset", "medium", "-crf", "26", "-pix_fmt", "yuv420p",
                    "-movflags", "+faststart", native_path(video, binary)], check=True)
    if video.stat().st_size >= 24 * 1024 * 1024:
        raise ValueError("Recording exceeds the 24 MiB publication bound")
    subprocess.run([str(binary), "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
                    "-sseof", "-2", "-i", native_path(video, binary), "-frames:v", "1",
                    "-update", "1", native_path(output / "poster.png", binary)], check=True)
    pnl = evidence["portfolio"]["dayPnl"]
    (output / "transcript.txt").write_text(
        "Full stack running locally in simulated/MOCK mode.\n\n"
        "MarketPulse displays simulated live quotes and Gateway REST candles. A BUY is submitted through the order ticket, "
        "then the authoritative execution appears in Session Activity. A SELL closes the same position through the normal flow.\n\n"
        f"Core calculates Day P&L as {pnl['value']} {pnl['currency']} for {evidence['newYorkDay']} in America/New_York, "
        "including commissions. The browser value is checked against Core.\n\n"
        "The News tab shows synthetic news and synthetic-news.v1 interpretations. Session Activity retains the real simulated execution notifications.\n")
    (output / "verified-recording.json").write_text(json.dumps({
        "scenario_passed": True, "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "video_sha256": hashlib.sha256(video.read_bytes()).hexdigest(), "evidence": evidence}, indent=2))
    print(f"Chronological silent MP4 encoded: {video.stat().st_size} bytes")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ffmpeg", type=Path, required=True)
    args = parser.parse_args()
    encode(args.source, args.output, args.ffmpeg)
