# TopVenues brand

TopVenues is scientific infrastructure: a corpus that many venues feed and one
snapshot freezes. The brand says exactly that, with one mark, three blues and
one typeface.

## The mark

The V of Venues is a lattice. Two strips lean one way and two the other, like
venue streams converging; where they cross, a lighter diamond and a darker
triangle form the point: one corpus. The mark replaces the V in the wordmark and
stands alone as the app icon. It is drawn from straight lines at one angle, so it
stays crisp from a 16 px favicon to a stage banner.

| File | Use |
| --- | --- |
| `topvenues-wordmark.svg` | Primary logo, light backgrounds |
| `topvenues-wordmark-reversed.svg` | Primary logo, dark backgrounds |
| `topvenues-wordmark-mono.svg` | One colour: print, embossing, stamps |
| `topvenues-mark.svg` | The V alone, light backgrounds |
| `topvenues-mark-reversed.svg` | The V alone, dark backgrounds |
| `topvenues-mark-mono.svg` | The V alone, one colour |
| `topvenues-app-icon.svg` | App icon, favicon, avatar, social |
| `topvenues-social.svg` | 1280×640 link preview |
| `mark-{16,32,48,180,512}.png` | Raster sizes of the app icon |
| `topvenues-wordmark.png`, `topvenues-wordmark-reversed.png`, `topvenues-mark.png`, `topvenues-social.png` | Raster copies for tools that cannot read SVG |

SVG is the master; PNG is a distribution format. The wordmark glyphs are Inter
Black outlines, so every file renders the same without the font installed.
`build_brand.py` regenerates every SVG from the lattice geometry and the font:

```bash
python docs/brand/build_brand.py --font Inter-Black.ttf --text-font Inter-Medium.ttf --out docs/brand
```

## Colour

### Primary

| Token | Hex | Role |
| --- | --- | --- |
| Ink Navy | `#10233F` | "Top", headings, dark surfaces |
| Corpus Blue | `#2867B2` | "enues", actions, links, highlights |
| Sky Blue | `#4DA3FF` | Secondary accent; the accent on dark surfaces |

### Neutral

| Token | Hex | Role |
| --- | --- | --- |
| Charcoal | `#1F2937` | Secondary text |
| Slate | `#667085` | Muted text, borders |
| Mist | `#E9EEF4` | Surfaces, quiet backgrounds |
| Paper | `#FFFFFF` | Base |

### The mark's ramp

| Piece | Light | Reversed |
| --- | --- | --- |
| Outer left strip | `#4DA3FF` | `#8CC4FF` |
| Inner left strip | `#2F86EA` | `#4DA3FF` |
| Inner right strip | `#2867B2` | `#2F86EA` |
| Outer right strip | `#10233F` | `#F4F7FB` |
| Diamond | `#A6CFFF` | `#D2E7FF` |
| Triangle | `#1C4F8F` | `#8CC4FF` |

The outer right strip always takes the colour of "Top", so the lockup stays one
object in both variants.

### Dark surfaces

Derived, not inverted: Night `#0B1627` for the page, Night Surface `#122238` for
panels, `#E8EDF4` for text, `#9AA7B8` for muted text, Sky Blue for accents.

### Contrast, measured

| Foreground / background | Ratio | Use |
| --- | --- | --- |
| Ink Navy / Paper | 15.74:1 | Any text |
| Charcoal / Paper | 14.68:1 | Any text |
| Corpus Blue / Paper | 5.72:1 | Text, links |
| Slate / Paper | 4.97:1 | Text |
| Ink Navy / Mist | 13.49:1 | Any text |
| Sky Blue / Paper | 2.63:1 | Graphics only, never text on light |
| Sky Blue / Ink Navy | 5.99:1 | Text and accents on dark |
| `#E8EDF4` / Night | 15.41:1 | Text on dark |
| `#9AA7B8` / Night | 7.42:1 | Muted text on dark |

## Brand colour is not data meaning

The blues identify TopVenues. In a chart they identify a series, and nothing
more: they never mean *better*, *significant*, *current* or *accepted*. The
palette is declared once, in `web/theme.py`, for both themes. No chart relies on
colour alone: direct labels, line dash or markers carry the distinction too.

| Role | Light | Dark | Contrast (light / dark) |
| --- | --- | --- | --- |
| Series 1, and every single-series chart | `#2867B2` Corpus Blue | `#4D8FE0` | 5.72:1 / 5.46:1 |
| Series 2 | `#EB6834` | `#D95926` | ≥ 3:1 / ≥ 3:1 |
| Series 3 | `#199E70` | `#199E70` | ≥ 3:1 / ≥ 3:1 |
| Partial category, such as the current year | `#89ABD5` | `#2C5283` | Always labelled "partial" |
| Track behind a part-of-a-whole bar | `#E9EEF4` Mist | `#1A2B44` | Recessive by design |

The three series pass every categorical check of the data-visualization
validator on their own surface, in both themes: lightness band, chroma floor,
colour-vision separation for all pairs (worst ΔE 8.4 light / 9.4 dark under
protanopia and deuteranopia) and normal-vision separation (worst ΔE 21.8 / 19.4).
`tests/test_theme_tokens.py` re-checks the contrast on every change.

Series colours fill marks and never set text; value labels use the muted text
colour. Coverage is drawn as a bar over a 100% track, so the missing stretch is
visible without reading the number.

## Type

Inter throughout. Black for the wordmark; Bold for H1 (56) and H2 (40);
SemiBold for H3 (28); Regular for body (16), small text (14) and captions (12).
Tabular figures wherever numbers are compared. Monospace only for commands,
identifiers and hashes. The interface self-hosts a subset of Inter from
`web/static/`; the project page carries a Latin subset in `site/assets/fonts/`.

## Brand line

*Reproducible research. Stronger insights.* Set in capitals, letter-spaced, in
Slate, under the wordmark on the link preview and title slides. It is a brand
line, not a result: it never replaces a measured claim.

## Use

- Clear space: at least half the height of the V on every side of the logo.
- Minimum size: wordmark 120 px wide; app icon 16 px.
- Use the reversed variants on Ink Navy or darker; never place the light
  wordmark on a dark background.
- Do not recolour, stretch, rotate, outline, add shadow or glow, or rebuild the
  wordmark in another typeface.
- One logo per surface. A slide or page does not need the logo repeated.
