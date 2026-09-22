"""Build timed narration and bilingual captions for the seven-minute demo."""

from __future__ import annotations

import json
import os
import re
import subprocess
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CUES_PATH = ROOT / "docs" / "demo" / "cues.json"
OUTPUT_DIR = ROOT / "output" / "demo"
TARGET_CUE_SECONDS = 30.0


def _seconds(timestamp: str) -> float:
    hours, minutes, seconds = timestamp.split(":")
    return int(hours) * 3600 + int(minutes) * 60 + float(seconds)


def _timestamp(seconds: float, separator: str = ".") -> str:
    milliseconds = round(seconds * 1000)
    hours, remainder = divmod(milliseconds, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    whole_seconds, milliseconds = divmod(remainder, 1000)
    return f"{hours:02d}:{minutes:02d}:{whole_seconds:02d}{separator}{milliseconds:03d}"


def _audio_duration(path: Path) -> float:
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return float(result.stdout.strip())


def _request_speech(text: str, output_path: Path) -> None:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required to build narration")
    request = urllib.request.Request(
        "https://api.openai.com/v1/audio/speech",
        data=json.dumps(
            {
                "model": "gpt-4o-mini-tts",
                "voice": "cedar",
                "input": text,
                "instructions": (
                    "Natural, fluent US English. Sound like a calm senior researcher giving a "
                    "live software demonstration. Use a measured pace near 130 words per minute, "
                    "brief pauses between sentences, and understated confidence."
                ),
                "response_format": "wav",
            }
        ).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        output_path.write_bytes(response.read())


def _fit_audio(source: Path, target: Path) -> float:
    source_duration = _audio_duration(source)
    filters: list[str] = []
    spoken_duration = source_duration
    if source_duration > TARGET_CUE_SECONDS - 0.5:
        speed = source_duration / (TARGET_CUE_SECONDS - 0.5)
        filters.append(f"atempo={speed:.8f}")
        spoken_duration = TARGET_CUE_SECONDS - 0.5
    filters.extend(["apad", f"atrim=0:{TARGET_CUE_SECONDS}"])
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-loglevel",
            "error",
            "-i",
            str(source),
            "-af",
            ",".join(filters),
            "-ar",
            "48000",
            "-ac",
            "1",
            str(target),
        ],
        check=True,
    )
    return spoken_duration


def _sentences(text: str) -> list[str]:
    return [part.strip() for part in re.split(r"(?<=[.!?])\s+", text) if part.strip()]


def _caption_entries(
    cues: list[dict[str, str]], language: str, spoken_durations: list[float]
) -> list[tuple[float, float, str]]:
    entries: list[tuple[float, float, str]] = []
    for cue, spoken_duration in zip(cues, spoken_durations, strict=True):
        cue_start = _seconds(cue["start"])
        sentences = _sentences(cue[language])
        word_counts = [max(len(sentence.split()), 1) for sentence in sentences]
        total_words = sum(word_counts)
        cursor = cue_start
        for sentence, word_count in zip(sentences, word_counts, strict=True):
            duration = spoken_duration * word_count / total_words
            entries.append((cursor, cursor + duration, sentence))
            cursor += duration
    return entries


def _write_vtt(path: Path, entries: list[tuple[float, float, str]]) -> None:
    lines = ["WEBVTT", ""]
    for start, end, text in entries:
        lines.extend([f"{_timestamp(start)} --> {_timestamp(end)}", text, ""])
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_srt(path: Path, entries: list[tuple[float, float, str]]) -> None:
    lines: list[str] = []
    for index, (start, end, text) in enumerate(entries, start=1):
        lines.extend(
            [
                str(index),
                f"{_timestamp(start, ',')} --> {_timestamp(end, ',')}",
                text,
                "",
            ]
        )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    cues = json.loads(CUES_PATH.read_text(encoding="utf-8"))
    fitted_paths: list[Path] = []
    spoken_durations: list[float] = []
    for index, cue in enumerate(cues, start=1):
        raw_path = OUTPUT_DIR / f"cue-{index:02d}-raw.wav"
        fitted_path = OUTPUT_DIR / f"cue-{index:02d}.wav"
        if not raw_path.exists():
            _request_speech(cue["en"], raw_path)
        spoken_durations.append(_fit_audio(raw_path, fitted_path))
        fitted_paths.append(fitted_path)

    concat_path = OUTPUT_DIR / "narration-files.txt"
    concat_path.write_text(
        "".join(f"file '{path.name}'\n" for path in fitted_paths), encoding="utf-8"
    )
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-loglevel",
            "error",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(concat_path),
            "-c",
            "pcm_s16le",
            str(OUTPUT_DIR / "narration.en.wav"),
        ],
        check=True,
    )

    for language, filename in (("en", "captions.en"), ("pt", "captions.pt-BR")):
        entries = _caption_entries(cues, language, spoken_durations)
        _write_vtt(OUTPUT_DIR / f"{filename}.vtt", entries)
        _write_srt(OUTPUT_DIR / f"{filename}.srt", entries)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
