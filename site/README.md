# Project page

Source of <https://sidneibarbieri.github.io/topVenues/>, the front door to
TopVenues: the repository, both papers and the dataset.

```bash
python site/build_site.py --out /tmp/topvenues-site
```

The build needs only the standard library and Pydantic, which the pinned
requirements already install. Every figure about the current release is read
from `data/profiles/security-20-v5/manifest.json`, the venue
areas from `src/areas.py`, and the abstract-audit result from
`evaluation/security-20-v3/manual_abstract_audit_summary.json`. The two papers
are frozen publications, so their values are constants in `build_site.py`; a
test checks them against `docs/PAPERS.md`. Brand files come from `docs/brand/`
and screenshots from `docs/assets/screenshots/`.

The page is bilingual (English and Brazilian Portuguese; `?lang=pt` selects
Portuguese), follows the system light or dark setting, sets no cookies, and
loads nothing from third parties until the demo video is played. Inter is
self-hosted as a Latin subset under the SIL Open Font License
(`assets/fonts/Inter-LICENSE.txt`).

## Publishing

The page is served from this repository's `gh-pages` branch, so `main` never
carries build output. Build into a checkout of that branch, commit, and push:

```bash
git worktree add ../topvenues-pages gh-pages
python site/build_site.py --out ../topvenues-pages
git -C ../topvenues-pages add -A
git -C ../topvenues-pages commit -m "Rebuild from topVenues vX.Y.Z"
git -C ../topvenues-pages push
```
