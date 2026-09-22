# gh-pages: generated project page

This branch serves <https://sidneibarbieri.github.io/topVenues/>. It is
generated, not edited: the source is [`site/`](https://github.com/sidneibarbieri/topVenues/tree/main/site)
on `main`, and every figure about the current release is read from the
profile manifest when the page is built:

```bash
python site/build_site.py --out <gh-pages worktree>
```

The main-track paper's artifact is frozen at the
[`sbseg2026-camera-ready`](https://github.com/sidneibarbieri/topVenues/tree/sbseg2026-camera-ready)
release and is not affected by this branch.
