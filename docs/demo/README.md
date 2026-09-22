# Demonstration

[`topvenues-demo-v1.5.9.mp4`](https://huggingface.co/datasets/sidneibarbieri/topvenues/resolve/main/assets/demo/topvenues-demo-v1.5.9.mp4) —
7:49, 1920x1080, 30 fps, recorded with v1.5.9, before the current visual identity.

[![Demonstration poster](../assets/demos/posters/topvenues-demo-v1.5.9.jpg)](https://huggingface.co/datasets/sidneibarbieri/topvenues/resolve/main/assets/demo/topvenues-demo-v1.5.9.mp4)

## Tracks

Narration is US English. Captions are embedded as two tracks, Brazilian
Portuguese first and default, English second. The same captions ship as
sidecar SRT files in [`../assets/demos/captions/`](../assets/demos/captions/).

## Source

- `narration.json` — every segment, its text in both languages, its measured
  audio duration, and its position in the finished cut.
- `SHOT_PLAN.md` — the act structure and what each act proves.

Timing is measured, not planned: each segment was synthesized first and the
shot lengths derived from the resulting audio. A fixed thirty-second grid,
used in the previous recording, left sixteen percent of the runtime silent
with gaps up to six seconds.
