# Demonstration

[`topvenues-demo-v1.14.0.mp4`](https://huggingface.co/datasets/sidneibarbieri/topvenues/resolve/main/assets/demo/topvenues-demo-v1.14.0.mp4) —
2:32, 1920x1080, 30 fps, showing v1.14.0 on the `security-20-v5` profile. The
narration states that profile's counts, so a release that changes them needs a
new recording.

[![Demonstration poster](../assets/demos/posters/topvenues-demo-v1.14.0.jpg)](https://huggingface.co/datasets/sidneibarbieri/topvenues/resolve/main/assets/demo/topvenues-demo-v1.14.0.mp4)

## The three files

| File | Captions |
| --- | --- |
| [`topvenues-demo-v1.14.0.mp4`](https://huggingface.co/datasets/sidneibarbieri/topvenues/resolve/main/assets/demo/topvenues-demo-v1.14.0.mp4) | none; load the WebVTT tracks below |
| [`topvenues-demo-v1.14.0.en.mp4`](https://huggingface.co/datasets/sidneibarbieri/topvenues/resolve/main/assets/demo/topvenues-demo-v1.14.0.en.mp4) | English, burned in |
| [`topvenues-demo-v1.14.0.pt-BR.mp4`](https://huggingface.co/datasets/sidneibarbieri/topvenues/resolve/main/assets/demo/topvenues-demo-v1.14.0.pt-BR.mp4) | Brazilian Portuguese, burned in |

Narration is English. The WebVTT files are in
[`../assets/demos/captions/`](../assets/demos/captions/), and a feed that plays
muted is what the burned-in files are for.

## Source

`narration.json` holds every segment: its spoken text, its caption in both
languages, and the **measured** duration of its synthesized audio.

Timing is measured, not planned. Each segment is synthesized first and the shot
lengths derive from the resulting audio, so the picture cannot drift from the
voice. The picture itself is rendered frame by frame at twice the delivered
resolution rather than screen-recorded, which is what keeps the interface text
legible. The build is in the research tree, under `video/build-v4/`.
