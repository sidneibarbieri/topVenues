"""Build the TopVenues brand masters as SVG.

The mark is a lattice: two strips lean left, two lean right, and the pieces
are the convex polygons where they run or cross. Every glyph of the wordmark is
an outline taken from Inter Black, so the SVG renders the same without the font
installed. The SVG files are the masters; PNG files are rasterized from them.

Usage: python build_brand.py --font Inter-Black.ttf --text-font Inter-Medium.ttf --out DIRECTORY
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.transformPen import TransformPen
from fontTools.ttLib import TTFont

Point = tuple[float, float]
Polygon = list[Point]

PIECE_ORDER = ("a", "diamond", "triangle", "b", "c", "d")


@dataclass(frozen=True)
class Palette:
    """Brand tokens, as specified on the TopVenues brand board."""

    ink_navy: str = "#10233F"
    corpus_blue: str = "#2867B2"
    sky_blue: str = "#4DA3FF"
    charcoal: str = "#1F2937"
    slate: str = "#667085"
    mist: str = "#E9EEF4"
    paper: str = "#FFFFFF"


@dataclass(frozen=True)
class Scheme:
    """Colours of one variant: the two wordmark halves and each mark piece."""

    top: str
    venues: str
    pieces: dict[str, str]


PALETTE = Palette()

SCHEMES = {
    "light": Scheme(
        top=PALETTE.ink_navy,
        venues=PALETTE.corpus_blue,
        pieces={
            "a": PALETTE.sky_blue,
            "b": "#2F86EA",
            "c": PALETTE.corpus_blue,
            "d": PALETTE.ink_navy,
            "diamond": "#A6CFFF",
            "triangle": "#1C4F8F",
        },
    ),
    "reversed": Scheme(
        top="#F4F7FB",
        venues=PALETTE.sky_blue,
        pieces={
            "a": "#8CC4FF",
            "b": PALETTE.sky_blue,
            "c": "#2F86EA",
            "d": "#F4F7FB",
            "diamond": "#D2E7FF",
            "triangle": "#8CC4FF",
        },
    ),
    "mono": Scheme(top="#000000", venues="#000000", pieces=dict.fromkeys(PIECE_ORDER, "#000000")),
}


@dataclass(frozen=True)
class Lattice:
    """The mark's geometry, measured on the brand board and regularized.

    Strip bounds are horizontal positions at the top edge; every strip leans
    with the same slope, so all gaps and angles are uniform.
    """

    top: float = 240.0
    bottom: float = 1049.0
    slope: float = 0.61
    strip_a: tuple[float, float] = (50.0, 247.0)
    strip_b: tuple[float, float] = (296.0, 490.0)
    strip_c: tuple[float, float] = (816.0, 996.0)
    strip_d: tuple[float, float] = (1049.0, 1239.0)
    cut_gap: float = 24.0

    @property
    def left(self) -> float:
        return self.strip_a[0]

    @property
    def width(self) -> float:
        return self.strip_d[1] - self.strip_a[0]

    @property
    def height(self) -> float:
        return self.bottom - self.top

    def along_left(self, point: Point) -> float:
        """Top-edge position of the left-leaning line through a point."""
        return point[0] - self.slope * (point[1] - self.top)

    def along_right(self, point: Point) -> float:
        """Top-edge position of the right-leaning line through a point."""
        return point[0] + self.slope * (point[1] - self.top)


# ------------------------------------------------------------------ geometry


def clip(
    polygon: Polygon, measure: Callable[[Point], float], bound: float, keep_below: bool
) -> Polygon:
    """Keep the part of a convex polygon on one side of a line (Sutherland-Hodgman)."""

    def inside(point: Point) -> bool:
        return measure(point) <= bound if keep_below else measure(point) >= bound

    def crossing(start: Point, end: Point) -> Point:
        ratio = (bound - measure(start)) / (measure(end) - measure(start))
        return (start[0] + ratio * (end[0] - start[0]), start[1] + ratio * (end[1] - start[1]))

    clipped: Polygon = []
    for index, current in enumerate(polygon):
        previous = polygon[index - 1]
        if inside(current):
            if not inside(previous):
                clipped.append(crossing(previous, current))
            clipped.append(current)
        elif inside(previous):
            clipped.append(crossing(previous, current))
    return clipped


def between(
    polygon: Polygon, measure: Callable[[Point], float], low: float | None, high: float | None
) -> Polygon:
    if low is not None:
        polygon = clip(polygon, measure, low, keep_below=False)
    if high is not None:
        polygon = clip(polygon, measure, high, keep_below=True)
    return polygon


def band(lattice: Lattice) -> Polygon:
    wide = 10 * lattice.width
    return [
        (-wide, lattice.top),
        (wide, lattice.top),
        (wide, lattice.bottom),
        (-wide, lattice.bottom),
    ]


def mark_pieces(lattice: Lattice) -> dict[str, Polygon]:
    """Strip A stops before C and reappears where it crosses C and D; B runs over C and D."""
    gap = lattice.cut_gap
    strip_a = between(band(lattice), lattice.along_left, *lattice.strip_a)
    strip_b = between(band(lattice), lattice.along_left, *lattice.strip_b)
    strip_c = between(band(lattice), lattice.along_right, *lattice.strip_c)
    strip_d = between(band(lattice), lattice.along_right, *lattice.strip_d)
    after_b = lattice.strip_b[1] + gap
    return {
        "a": between(strip_a, lattice.along_right, None, lattice.strip_c[0] - gap),
        "diamond": between(strip_a, lattice.along_right, *lattice.strip_c),
        "triangle": between(strip_a, lattice.along_right, *lattice.strip_d),
        "b": between(strip_b, lattice.along_right, None, lattice.strip_d[1] - gap),
        "c": between(strip_c, lattice.along_left, after_b, None),
        "d": between(strip_d, lattice.along_left, after_b, None),
    }


def place(polygon: Polygon, lattice: Lattice, height: float, origin: Point) -> Polygon:
    """Scale the mark so its height matches, with its top-left corner at origin."""
    scale = height / lattice.height
    return [
        (
            origin[0] + (horizontal - lattice.left) * scale,
            origin[1] + (vertical - lattice.top) * scale,
        )
        for horizontal, vertical in polygon
    ]


def mark_width(lattice: Lattice, height: float) -> float:
    return lattice.width * height / lattice.height


# ------------------------------------------------------------------ svg


def polygon_element(polygon: Polygon, colour: str) -> str:
    points = " ".join(f"{horizontal:.2f},{vertical:.2f}" for horizontal, vertical in polygon)
    return f'<polygon points="{points}" fill="{colour}"/>'


def mark_elements(scheme: Scheme, lattice: Lattice, height: float, origin: Point) -> list[str]:
    pieces = mark_pieces(lattice)
    return [
        polygon_element(place(pieces[name], lattice, height, origin), scheme.pieces[name])
        for name in PIECE_ORDER
    ]


def svg_document(width: float, height: float, body: list[str], label: str) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width:.2f} {height:.2f}" '
        f'width="{width:.0f}" height="{height:.0f}" role="img" aria-label="{label}">\n'
        f"  <title>{label}</title>\n  " + "\n  ".join(body) + "\n</svg>\n"
    )


@dataclass(frozen=True)
class Typeface:
    """Glyph outlines from one font file."""

    path: Path

    def outline(
        self, text: str, size: float, x_start: float, baseline: float, tracking: float
    ) -> tuple[list[str], float]:
        """SVG path data for each glyph, and the pen position after the last one."""
        font = TTFont(self.path)
        glyph_set = font.getGlyphSet()
        character_map = font.getBestCmap()
        scale = size / font["head"].unitsPerEm
        paths, pen_x = [], x_start
        for character in text:
            glyph_name = character_map[ord(character)]
            pen = SVGPathPen(glyph_set)
            glyph_set[glyph_name].draw(TransformPen(pen, (scale, 0, 0, -scale, pen_x, baseline)))
            paths.append(pen.getCommands())
            pen_x += glyph_set[glyph_name].width * scale + tracking * size
        return paths, pen_x

    def cap_height(self) -> float:
        font = TTFont(self.path)
        return font["OS/2"].sCapHeight / font["head"].unitsPerEm


def path_elements(paths: list[str], colour: str) -> list[str]:
    return [f'<path d="{data}" fill="{colour}"/>' for data in paths]


@dataclass(frozen=True)
class WordmarkLayout:
    """Proportions of the lockup, in em units of the wordmark size."""

    size: float = 100.0
    tracking: float = -0.025
    gap_before_mark: float = -0.03
    gap_after_mark: float = -0.10
    margin: float = 0.06


def wordmark(scheme: Scheme, typeface: Typeface, lattice: Lattice, layout: WordmarkLayout) -> str:
    """'Top' + the mark as the capital V + 'enues', on one baseline."""
    size = layout.size
    cap = typeface.cap_height() * size
    margin = layout.margin * size
    baseline = margin + cap
    top_paths, pen_x = typeface.outline("Top", size, margin, baseline, layout.tracking)
    mark_x = pen_x + layout.gap_before_mark * size
    mark = mark_elements(scheme, lattice, cap, (mark_x, baseline - cap))
    venues_x = mark_x + mark_width(lattice, cap) + layout.gap_after_mark * size
    venues_paths, pen_x = typeface.outline("enues", size, venues_x, baseline, layout.tracking)
    descender = 0.22 * size
    body = path_elements(top_paths, scheme.top) + mark + path_elements(venues_paths, scheme.venues)
    return svg_document(pen_x + margin, baseline + descender + margin, body, "TopVenues")


def standalone_mark(scheme: Scheme, lattice: Lattice, height: float = 256.0) -> str:
    padding = 0.08 * height
    width = mark_width(lattice, height)
    body = mark_elements(scheme, lattice, height, (padding, padding))
    return svg_document(width + 2 * padding, height + 2 * padding, body, "TopVenues mark")


def app_icon(lattice: Lattice, size: float = 512.0) -> str:
    """The reversed mark centred on an Ink Navy rounded square."""
    mark_height = 0.5 * size
    width = mark_width(lattice, mark_height)
    origin = ((size - width) / 2, (size - mark_height) / 2 + 0.01 * size)
    body = [
        f'<rect width="{size:.0f}" height="{size:.0f}" rx="{0.22 * size:.1f}" fill="{PALETTE.ink_navy}"/>'
    ]
    body += mark_elements(SCHEMES["reversed"], lattice, mark_height, origin)
    return svg_document(size, size, body, "TopVenues")


def social_card(display: Typeface, text: Typeface, lattice: Lattice) -> str:
    """1280x640 link preview: the wordmark and the brand line."""
    inner = wordmark(SCHEMES["light"], display, lattice, WordmarkLayout(size=170.0, margin=0.0))
    width, height = (
        float(value) for value in inner.split('viewBox="0 0 ')[1].split('"')[0].split()
    )
    left, top = (1280 - width) / 2, 215.0
    content = inner.split("</title>\n", 1)[1].rsplit("</svg>", 1)[0]
    line = "REPRODUCIBLE RESEARCH. STRONGER INSIGHTS."
    _, line_width = text.outline(line, 26, 0, 0, 0.27)
    line_paths, _ = text.outline(line, 26, (1280 - line_width) / 2, top + height + 58, 0.27)
    body = [
        f'<rect width="1280" height="640" fill="{PALETTE.paper}"/>',
        f'<g transform="translate({left:.1f} {top:.1f})">{content}</g>',
        *path_elements(line_paths, PALETTE.slate),
        f'<rect y="600" width="1280" height="40" fill="{PALETTE.ink_navy}"/>',
    ]
    return svg_document(1280, 640, body, "TopVenues")


def build(display_font: Path, text_font: Path, out: Path) -> list[Path]:
    typeface, lattice, layout = Typeface(display_font), Lattice(), WordmarkLayout()
    out.mkdir(parents=True, exist_ok=True)
    documents = {
        "topvenues-wordmark.svg": wordmark(SCHEMES["light"], typeface, lattice, layout),
        "topvenues-wordmark-reversed.svg": wordmark(SCHEMES["reversed"], typeface, lattice, layout),
        "topvenues-wordmark-mono.svg": wordmark(SCHEMES["mono"], typeface, lattice, layout),
        "topvenues-mark.svg": standalone_mark(SCHEMES["light"], lattice),
        "topvenues-mark-reversed.svg": standalone_mark(SCHEMES["reversed"], lattice),
        "topvenues-mark-mono.svg": standalone_mark(SCHEMES["mono"], lattice),
        "topvenues-app-icon.svg": app_icon(lattice),
        "topvenues-social.svg": social_card(typeface, Typeface(text_font), lattice),
    }
    written = []
    for name, document in documents.items():
        (out / name).write_text(document)
        written.append(out / name)
    return written


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--font", type=Path, required=True, help="Inter-Black.ttf (Inter 4.1, SIL OFL)"
    )
    parser.add_argument("--text-font", type=Path, required=True, help="Inter-Medium.ttf")
    parser.add_argument("--out", type=Path, default=Path(__file__).parent)
    arguments = parser.parse_args()
    for path in build(arguments.font, arguments.text_font, arguments.out):
        print(f"wrote {path.name}")
