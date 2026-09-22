#!/usr/bin/env python3
"""Build the TopVenues project page from this repository's release data.

Every figure about the current release is read at build time: the profile
manifest, ``pyproject.toml``, ``src/areas.py`` and the audit summary. The two
papers are frozen publications, so their values are constants here; a test
checks them against ``docs/PAPERS.md``.

Usage: python site/build_site.py --out OUTPUT_DIR [--repository ROOT]
"""

import argparse
import datetime
import html
import importlib.util
import json
import math
import re
import shutil
import subprocess
import tomllib
from collections.abc import Callable
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

PROFILE = "security-20-v4"
SITE_DIR = Path(__file__).resolve().parent
SITE_URL = "https://sidneibarbieri.github.io/topVenues/"
GITHUB = "https://github.com/sidneibarbieri"
REPOSITORY_URL = f"{GITHUB}/topVenues"
HUGGING_FACE = "https://huggingface.co/datasets/sidneibarbieri"
DEMO_VIDEO = f"{HUGGING_FACE}/topvenues/resolve/main/assets/demo/topvenues-demo-v1.12.0"
AUTHORS = ("Sidnei Barbieri", "Ágney Lopes Roth Ferraz", "Lourenço Alves Pereira Júnior")
BIBTEX_AUTHORS = "Sidnei Barbieri and {\\'A}gney Lopes Roth Ferraz and Louren{\\c{c}}o Alves {Pereira J{\\'u}nior}"
BRAND_FILES = ("topvenues-app-icon.svg", "topvenues-social.png", "mark-32.png", "mark-180.png")


class Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")


class CommandBlock(Frozen):
    label_en: str
    label_pt: str
    text: str


class Paper(Frozen):
    """A published paper and the artifact that reproduces it."""

    anchor: str
    kind_en: str
    kind_pt: str
    title: str
    booktitle: str
    pages: str
    sol_url: str
    doi: str
    repository: str
    repository_note_en: str = ""
    repository_note_pt: str = ""
    release: str
    profile: str | None = None
    records: int
    venues: int
    window: str | None = None
    snapshot_date_en: str | None = None
    snapshot_date_pt: str | None = None
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    tests: int | None = None
    commands: tuple[CommandBlock, ...]


PAPER_A = Paper(
    anchor="paper-a",
    kind_en="Paper A · Main track, SBSeg 2026",
    kind_pt="Artigo A · Trilha principal, SBSeg 2026",
    title="TopVenues: A Reproducible Corpus and Tooling Substrate for Cybersecurity Literature Reviews",
    booktitle="Anais do XXVI Simpósio Brasileiro de Cibersegurança (SBSeg 2026)",
    pages="1150–1165",
    sol_url="https://sol.sbc.org.br/index.php/sbseg/article/view/44350",
    doi="10.5753/sbseg.2026.29056",
    repository="topVenues",
    release="sbseg2026-camera-ready",
    records=9925,
    venues=11,
    snapshot_date_en="May 2026",
    snapshot_date_pt="maio de 2026",
    sha256="0f4dbaa97d0cf39abd2340adb3280643df090b5de9cd1a29bff39a0b53ef64cd",
    tests=252,
    commands=(
        CommandBlock(
            label_en="Reproduce",
            label_pt="Reproduzir",
            text="git clone https://github.com/sidneibarbieri/topVenues\ncd topVenues\n"
            "git checkout sbseg2026-camera-ready\nbash reproduce.sh",
        ),
    ),
)

PAPER_B = Paper(
    anchor="paper-b",
    kind_en="Paper B · Tools track, SBSeg 2026",
    kind_pt="Artigo B · Salão de Ferramentas, SBSeg 2026",
    title="TopVenues: An Executable Corpus and Research Tool for Cybersecurity Literature Reviews",
    booktitle="Anais Estendidos do XXVI Simpósio Brasileiro de Cibersegurança (SBSeg 2026)",
    pages="234–241",
    sol_url="https://sol.sbc.org.br/index.php/sbseg_estendido/article/view/44470",
    doi="10.5753/sbseg_estendido.2026.33733",
    repository="topvenues-tool",
    repository_note_en="printed in the paper; archived, read-only",
    repository_note_pt="impresso no artigo; arquivado, somente leitura",
    release="sbseg2026-sf-submission-r1",
    profile="security-20",
    records=20305,
    venues=20,
    window="2017–2026",
    sha256="5a35bd6e3ec6845a0fde4cc3d6aa05b1db04e511cb39e783eeaee2cea7493b08",
    commands=(
        CommandBlock(
            label_en="Printed in the paper",
            label_pt="Impresso no artigo",
            text="git clone https://github.com/sidneibarbieri/topvenues-tool\ncd topvenues-tool\n"
            "bash reproduce.sh --profile security-20",
        ),
        CommandBlock(
            label_en="Same snapshot, here",
            label_pt="Mesmo snapshot, aqui",
            text="git clone https://github.com/sidneibarbieri/topVenues\ncd topVenues\n"
            "bash reproduce.sh --profile security-20",
        ),
    ),
)

PAPERS = (PAPER_A, PAPER_B)


class VenueCount(Frozen):
    event: str
    papers: int
    abstracts: int
    bibtex: int
    year_min: int
    year_max: int


class Snapshot(Frozen):
    papers: int
    abstracts: int
    bibtex: int
    venues: int
    observed_year_min: int
    observed_year_max: int
    gzip_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    event_counts: tuple[VenueCount, ...]


class Manifest(Frozen):
    profile_id: str
    built_on: str
    snapshot: Snapshot


class AuditSummary(Frozen):
    labelled: int
    usable: int
    wilson_95_ci: tuple[float, float]


class Venue(Frozen):
    count: VenueCount
    area: str

    @property
    def is_security(self) -> bool:
        return self.area == "security"


class Release(Frozen):
    """Everything the page states about the current release."""

    manifest: Manifest
    version: str
    commit: str
    audit: AuditSummary
    venues: tuple[Venue, ...]

    @property
    def snapshot(self) -> Snapshot:
        return self.manifest.snapshot

    @property
    def security_venues(self) -> tuple[Venue, ...]:
        return tuple(venue for venue in self.venues if venue.is_security)

    @property
    def survey_venues(self) -> tuple[Venue, ...]:
        return tuple(venue for venue in self.venues if not venue.is_security)


# ------------------------------------------------------------------ loading


def load_area_function(repository: Path) -> Callable[[str], str]:
    """The tool's own venue→area mapping, loaded alone (src/__init__ imports the network stack)."""
    specification = importlib.util.spec_from_file_location(
        "topvenues_areas", repository / "src" / "areas.py"
    )
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module.area_for


def load_venues(snapshot: Snapshot, area_for: Callable[[str], str]) -> tuple[Venue, ...]:
    venues = [Venue(count=count, area=area_for(count.event)) for count in snapshot.event_counts]
    unmapped = [venue.count.event for venue in venues if venue.area == "unknown"]
    if unmapped:
        raise ValueError(f"venues without an area in src/areas.py: {unmapped}")
    return tuple(sorted(venues, key=lambda venue: (-venue.count.papers, venue.count.event)))


def current_commit(repository: Path) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repository), "rev-parse", "--short", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    )
    return completed.stdout.strip()


def load_release(repository: Path) -> Release:
    manifest = Manifest.model_validate_json(
        (repository / "data" / "profiles" / PROFILE / "manifest.json").read_text()
    )
    audit_path = repository / "evaluation" / "security-20-v3" / "manual_abstract_audit_summary.json"
    return Release(
        manifest=manifest,
        version=tomllib.loads((repository / "pyproject.toml").read_text())["project"]["version"],
        commit=current_commit(repository),
        audit=AuditSummary.model_validate_json(audit_path.read_text()),
        venues=load_venues(manifest.snapshot, load_area_function(repository)),
    )


# ------------------------------------------------------------------ formatting


def format_number(value: float, language: str, digits: int = 0) -> str:
    formatted = f"{value:,.{digits}f}"
    if language == "pt":
        formatted = formatted.replace(",", "\0").replace(".", ",").replace("\0", ".")
    return formatted


def format_percent(part: float, whole: float, language: str, digits: int = 1) -> str:
    return format_number(100 * part / whole, language, digits) + "%"


def escape(text: str) -> str:
    return html.escape(text, quote=True)


def bilingual(english: str, portuguese: str, tag: str = "span") -> str:
    """One element per language; the stylesheet shows the active one."""
    return f'<{tag} data-l="en">{english}</{tag}><{tag} data-l="pt">{portuguese}</{tag}>'


def bilingual_number(value: float, digits: int = 0) -> str:
    return bilingual(format_number(value, "en", digits), format_number(value, "pt", digits))


def code_block(text: str, label_en: str = "", label_pt: str = "") -> str:
    label = bilingual(label_en, label_pt) if label_en else ""
    return (
        f'<div class="code"><div class="code-bar"><span class="code-label">{label}</span>'
        f'<button class="copy" type="button" data-copy>{bilingual("Copy", "Copiar")}</button></div>'
        f"<pre><code>{escape(text)}</code></pre></div>"
    )


def inline_svg(path: Path, css_class: str) -> str:
    """A brand master inlined for the page, hidden from assistive technology."""
    svg = path.read_text()
    svg = re.sub(r"<title>.*?</title>\s*", "", svg)
    svg = re.sub(r'\s(width|height)="[^"]*"', "", svg, count=2)
    svg = re.sub(r'\srole="img" aria-label="[^"]*"', "", svg)
    return svg.replace(
        "<svg ", f'<svg class="{css_class}" aria-hidden="true" focusable="false" ', 1
    ).strip()


def png_size(path: Path) -> tuple[int, int]:
    """Width and height from the PNG header, so the page never guesses them."""
    header = path.read_bytes()[:24]
    return int.from_bytes(header[16:20], "big"), int.from_bytes(header[20:24], "big")


def size_screenshots(page: str, screenshots: Path) -> str:
    def with_real_size(match: re.Match) -> str:
        width, height = png_size(screenshots / match.group(1))
        return f'src="assets/screens/{match.group(1)}" width="{width}" height="{height}"'

    return re.sub(
        r'src="assets/screens/([\w./-]+\.png)" width="\d+" height="\d+"', with_real_size, page
    )


# ------------------------------------------------------------------ figure 1


FIGURE_TOP = 28
FIGURE_STEP = 24
CONVERGENCE_X = 560
SHORT_NAMES = {
    "IEEE Communications Surveys & Tutorials": "IEEE Comm. Surveys & Tutorials",
    "Foundations and Trends in Privacy and Security": "FnT Privacy and Security",
}


def venue_line(position: int, venue: Venue, largest: int, convergence_y: float) -> str:
    """One venue's label, count and curve towards the snapshot point.

    Line weight follows sqrt(records) so the smallest venues stay visible.
    pathLength drives the draw-in animation; it would rescale a dash pattern,
    so dashed (survey) lines fade in instead.
    """
    line_y = FIGURE_TOP + position * FIGURE_STEP
    weight = 0.8 + 4.4 * math.sqrt(venue.count.papers / largest)
    style = ' pathLength="1"' if venue.is_security else ' stroke-dasharray="5 4"'
    name = SHORT_NAMES.get(venue.count.event, venue.count.event)
    count = (
        f'<tspan data-l="en">{format_number(venue.count.papers, "en")}</tspan>'
        f'<tspan data-l="pt">{format_number(venue.count.papers, "pt")}</tspan>'
    )
    return (
        f'<g class="v" style="--i:{position}">'
        f'<text class="vn" x="232" y="{line_y + 4.5}" text-anchor="end">{escape(name)}</text>'
        f'<text class="vc" x="292" y="{line_y + 4.5}" text-anchor="end">{count}</text>'
        f'<path class="ln" d="M304 {line_y} C 430 {line_y}, 470 {convergence_y:.1f}, {CONVERGENCE_X} {convergence_y:.1f}" '
        f'stroke-width="{weight:.2f}"{style}/></g>'
    )


def snapshot_label(snapshot: Snapshot, convergence_y: float) -> str:
    years = f"{snapshot.observed_year_min}–{snapshot.observed_year_max}"
    digest = snapshot.gzip_sha256
    return (
        f'<g class="lbl" transform="translate({CONVERGENCE_X + 34} {convergence_y - 38:.1f})">'
        f'<text class="l1" y="0">{PROFILE}</text>'
        f'<text class="l2" y="26"><tspan data-l="en">{format_number(snapshot.papers, "en")} records</tspan>'
        f'<tspan data-l="pt">{format_number(snapshot.papers, "pt")} registros</tspan></text>'
        f'<text class="l3" y="48"><tspan data-l="en">{snapshot.venues} venues · {years}</tspan>'
        f'<tspan data-l="pt">{snapshot.venues} veículos · {years}</tspan></text>'
        f'<text class="l4" y="74">sha256 {digest[:8]}…{digest[-6:]}</text></g>'
    )


def convergence_figure(release: Release) -> str:
    """Figure 1: every declared venue converges on one snapshot. Colour carries no data."""
    venues = release.venues
    height = FIGURE_TOP * 2 + FIGURE_STEP * (len(venues) - 1)
    convergence_y = FIGURE_TOP + FIGURE_STEP * (len(venues) - 1) / 2
    largest = max(venue.count.papers for venue in venues)
    lines = [
        venue_line(position, venue, largest, convergence_y) for position, venue in enumerate(venues)
    ]
    return "\n".join(
        [
            f'<svg class="conv" viewBox="0 0 880 {height:.0f}" role="img" aria-labelledby="fig1-title" '
            'xmlns="http://www.w3.org/2000/svg">',
            f'<title id="fig1-title">{len(venues)} declared venues converge on one snapshot</title>',
            *lines,
            f'<circle class="halo" cx="{CONVERGENCE_X}" cy="{convergence_y:.1f}" r="17"/>',
            f'<circle class="pt" cx="{CONVERGENCE_X}" cy="{convergence_y:.1f}" r="8.5"/>',
            snapshot_label(release.snapshot, convergence_y),
            "</svg>",
        ]
    )


def venue_list(release: Release) -> str:
    """Narrow-screen text alternative to Figure 1."""
    survey_tag = f' <small class="tag">{bilingual("survey", "surveys")}</small>'
    items = "".join(
        f"<li><span>{escape(venue.count.event)}{'' if venue.is_security else survey_tag}</span>"
        f'<span class="n">{bilingual_number(venue.count.papers)}</span></li>'
        for venue in release.venues
    )
    return f'<ol class="vlist">{items}</ol>'


def venue_row(venue: Venue) -> str:
    count = venue.count
    area = (
        bilingual("security", "segurança") if venue.is_security else bilingual("survey", "surveys")
    )
    coverage = bilingual(
        format_percent(count.abstracts, count.papers, "en"),
        format_percent(count.abstracts, count.papers, "pt"),
    )
    return (
        f'<tr><th scope="row">{escape(count.event)}</th><td>{area}</td>'
        f'<td class="n">{bilingual_number(count.papers)}</td><td class="n">{bilingual_number(count.abstracts)}</td>'
        f'<td class="n">{coverage}</td><td class="n">{count.year_min}–{count.year_max}</td></tr>'
    )


def table_heading(english: str, portuguese: str, numeric: bool) -> str:
    css = ' class="n"' if numeric else ""
    return f'<th scope="col"{css}>{bilingual(english, portuguese)}</th>'


def venue_table(release: Release) -> str:
    head = "".join(
        table_heading(english, portuguese, numeric)
        for english, portuguese, numeric in (
            ("Venue", "Veículo", False),
            ("Area", "Área", False),
            ("Records", "Registros", True),
            ("Abstracts", "Resumos", True),
            ("Coverage", "Cobertura", True),
            ("Years", "Anos", True),
        )
    )
    rows = "".join(venue_row(venue) for venue in release.venues)
    return (
        f'<div class="table-wrap"><table class="venues"><thead><tr>{head}</tr></thead>'
        f"<tbody>{rows}</tbody></table></div>"
    )


# ------------------------------------------------------------------ papers


def snapshot_fact(paper: Paper) -> str:
    if paper.snapshot_date_en:
        return bilingual(
            f"{paper.snapshot_date_en} · {format_number(paper.records, 'en')} records · {paper.venues} venues",
            f"{paper.snapshot_date_pt} · {format_number(paper.records, 'pt')} registros · {paper.venues} veículos",
        )
    return bilingual(
        f"{format_number(paper.records, 'en')} records · {paper.venues} venues · {paper.window}",
        f"{format_number(paper.records, 'pt')} registros · {paper.venues} veículos · {paper.window}",
    )


def paper_facts(paper: Paper) -> str:
    repository_url = f"{GITHUB}/{paper.repository}"
    note = f' <span class="muted">({bilingual(paper.repository_note_en, paper.repository_note_pt)})</span>'
    facts = [
        (
            bilingual("Repository", "Repositório"),
            f'<a href="{repository_url}"><code>sidneibarbieri/{paper.repository}</code></a>'
            + (note if paper.repository_note_en else ""),
        ),
        (
            bilingual("Release", "Versão"),
            f'<a href="{repository_url}/releases/tag/{paper.release}"><code>{paper.release}</code></a>',
        ),
    ]
    if paper.profile:
        facts.append((bilingual("Profile", "Perfil"), f"<code>{paper.profile}</code>"))
    facts.append((bilingual("Snapshot", "Snapshot"), snapshot_fact(paper)))
    facts.append(("SHA-256", f'<code class="hash">{paper.sha256}</code>'))
    if paper.tests:
        facts.append(
            (
                bilingual("Tests", "Testes"),
                bilingual(f"{paper.tests} tests", f"{paper.tests} testes"),
            )
        )
    return "".join(f"<dt>{label}</dt><dd>{value}</dd>" for label, value in facts)


def paper_card(paper: Paper) -> str:
    commands = "".join(
        code_block(block.text, block.label_en, block.label_pt) for block in paper.commands
    )
    return f"""
<article class="paper" id="{paper.anchor}">
  <p class="kicker">{bilingual(paper.kind_en, paper.kind_pt)}</p>
  <h3>{escape(paper.title)}</h3>
  <p class="venue">{escape(paper.booktitle)}, {bilingual("pp.", "p.")} {paper.pages}</p>
  <p class="links"><a class="arrow" href="{paper.sol_url}">{bilingual("Read on SOL", "Ler no SOL")}</a>
     <span class="doi">DOI <code>{paper.doi}</code></span></p>
  <dl class="facts">{paper_facts(paper)}</dl>
  <div class="commands">{commands}</div>
</article>"""


def bibtex_entry(key: str, paper: Paper) -> str:
    booktitle = paper.booktitle.replace("Simpósio", "Simp{\\'o}sio").replace(
        "Cibersegurança", "Ciberseguran{\\c{c}}a"
    )
    title = "{TopVenues}" + paper.title[len("TopVenues") :]
    return (
        f"@inproceedings{{{key},\n"
        f"  author    = {{{BIBTEX_AUTHORS}}},\n"
        f"  title     = {{{title}}},\n"
        f"  booktitle = {{{booktitle}}},\n"
        f"  pages     = {{{paper.pages.replace('–', '--')}}},\n"
        "  year      = {2026},\n"
        "  publisher = {Sociedade Brasileira de Computa{\\c{c}}{\\~a}o},\n"
        "  address   = {Porto Alegre, RS, Brasil},\n"
        f"  doi       = {{{paper.doi}}},\n"
        f"  url       = {{{paper.sol_url}}}\n"
        "}"
    )


def structured_data(release: Release) -> str:
    snapshot = release.snapshot
    years = f"{snapshot.observed_year_min}–{snapshot.observed_year_max}"
    graph = [
        {
            "@type": "Dataset",
            "name": f"TopVenues cybersecurity corpus ({PROFILE})",
            "description": (
                f"{snapshot.papers:,} bibliographic records from {snapshot.venues} declared security and survey "
                f"venues, {years}, with abstracts where available and a BibTeX entry for every record."
            ),
            "url": f"{HUGGING_FACE}/topvenues",
            "sameAs": REPOSITORY_URL,
            "identifier": f"sha256:{snapshot.gzip_sha256}",
            "temporalCoverage": f"{snapshot.observed_year_min}/{snapshot.observed_year_max}",
            "creator": [{"@type": "Person", "name": author} for author in AUTHORS],
            "isAccessibleForFree": True,
        },
        {
            "@type": "SoftwareSourceCode",
            "name": "TopVenues",
            "codeRepository": REPOSITORY_URL,
            "programmingLanguage": "Python",
            "license": "https://opensource.org/licenses/MIT",
            "softwareVersion": release.version,
        },
        *(
            {
                "@type": "ScholarlyArticle",
                "headline": paper.title,
                "isPartOf": paper.booktitle,
                "url": paper.sol_url,
                "identifier": f"doi:{paper.doi}",
                "datePublished": "2026",
            }
            for paper in PAPERS
        ),
    ]
    return json.dumps(
        {"@context": "https://schema.org", "@graph": graph}, ensure_ascii=False, indent=1
    )


# ------------------------------------------------------------------ assembly


def quick_start_blocks(version: str) -> dict[str, str]:
    clone = f"git clone --depth 1 --branch v{version} {REPOSITORY_URL}.git\ncd topVenues\n"
    return {
        "{{QUICK_UNIX}}": code_block(clone + f"bash reproduce.sh --profile {PROFILE}"),
        "{{QUICK_WIN}}": code_block(
            clone + f"powershell -ExecutionPolicy Bypass -File .\\reproduce.ps1 -Profile {PROFILE}"
        ),
        "{{UI_UNIX}}": code_block("source .venv/bin/activate\npython -m streamlit run web/app.py"),
        "{{UI_WIN}}": code_block(
            ".\\.venv\\Scripts\\Activate.ps1\npython -m streamlit run web/app.py"
        ),
        "{{CLI}}": code_block(
            f'python -m src.cli --profile {PROFILE} search --rank "LLM security" \\\n'
            '  --tier-scope "Security top-4" --limit 20\n'
            f'python -m src.cli --profile {PROFILE} export --format bibtex --tech "fuzzing" \\\n'
            '  --tier-scope "Security top-4" -o fuzzing-tier1.bib'
        ),
        "{{HFCODE}}": code_block(
            "from datasets import load_dataset\n\n"
            'corpus = load_dataset("sidneibarbieri/topvenues", split="train")\n'
            'security = corpus.filter(lambda paper: paper["area"] == "security")'
        ),
    }


def release_values(release: Release) -> dict[str, str]:
    snapshot = release.snapshot
    esorics = next(venue.count for venue in release.venues if venue.count.event == "ESORICS")
    audit = release.audit
    interval_en = "–".join(
        format_number(100 * bound, "en", 1) + "%" for bound in audit.wilson_95_ci
    )
    interval_pt = "–".join(
        format_number(100 * bound, "pt", 1) + "%" for bound in audit.wilson_95_ci
    )
    security_records = sum(venue.count.papers for venue in release.security_venues)
    return {
        "{{RECORDS_EN}}": format_number(snapshot.papers, "en"),
        "{{RECORDS_PT}}": format_number(snapshot.papers, "pt"),
        "{{ABS_EN}}": format_number(snapshot.abstracts, "en"),
        "{{ABS_PT}}": format_number(snapshot.abstracts, "pt"),
        "{{ABSPCT_EN}}": format_percent(snapshot.abstracts, snapshot.papers, "en"),
        "{{ABSPCT_PT}}": format_percent(snapshot.abstracts, snapshot.papers, "pt"),
        "{{BIBPCT_EN}}": format_percent(snapshot.bibtex, snapshot.papers, "en", 0),
        "{{BIBPCT_PT}}": format_percent(snapshot.bibtex, snapshot.papers, "pt", 0),
        "{{VENUES}}": str(snapshot.venues),
        "{{CORE_N}}": str(len(release.security_venues)),
        "{{SURVEY_N}}": str(len(release.survey_venues)),
        "{{CORE_EN}}": format_number(security_records, "en"),
        "{{CORE_PT}}": format_number(security_records, "pt"),
        "{{SURVEYS}}": escape(", ".join(venue.count.event for venue in release.survey_venues)),
        "{{ESO_P_EN}}": format_number(esorics.papers, "en"),
        "{{ESO_P_PT}}": format_number(esorics.papers, "pt"),
        "{{ESO_A}}": str(esorics.abstracts),
        "{{AUD_N}}": str(audit.labelled),
        "{{AUD_EN}}": format_percent(audit.usable, audit.labelled, "en"),
        "{{AUD_PT}}": format_percent(audit.usable, audit.labelled, "pt"),
        "{{AUD_CI_EN}}": interval_en,
        "{{AUD_CI_PT}}": interval_pt,
        "{{Y0}}": str(snapshot.observed_year_min),
        "{{Y1}}": str(snapshot.observed_year_max),
        "{{SHA}}": snapshot.gzip_sha256,
        "{{PROFILE}}": PROFILE,
        "{{VERSION}}": release.version,
        "{{BUILT_ON}}": release.manifest.built_on,
    }


def page_values(release: Release, repository: Path, built_on: str) -> dict[str, str]:
    brand = repository / "docs" / "brand"
    return {
        "{{CSS}}": (SITE_DIR / "site.css").read_text(),
        "{{JS}}": (SITE_DIR / "site.js").read_text(),
        "{{BOOT}}": (SITE_DIR / "boot.js").read_text().strip(),
        "{{JSONLD}}": structured_data(release),
        "{{WORDMARK_LIGHT}}": inline_svg(brand / "topvenues-wordmark.svg", "on-light"),
        "{{WORDMARK_DARK}}": inline_svg(brand / "topvenues-wordmark-reversed.svg", "on-dark"),
        "{{MARK_LIGHT}}": inline_svg(brand / "topvenues-mark.svg", "on-light"),
        "{{MARK_DARK}}": inline_svg(brand / "topvenues-mark-reversed.svg", "on-dark"),
        "{{SKIP}}": bilingual("Skip to content", "Pular para o conteúdo"),
        "{{FIGURE}}": convergence_figure(release),
        "{{VLIST}}": venue_list(release),
        "{{VTABLE}}": venue_table(release),
        "{{PAPER_A}}": paper_card(PAPER_A),
        "{{PAPER_B}}": paper_card(PAPER_B),
        "{{BIB_A}}": code_block(
            bibtex_entry("barbieri2026topvenues", PAPER_A),
            "Paper A · method and measurements",
            "Artigo A · método e medições",
        ),
        "{{BIB_B}}": code_block(
            bibtex_entry("barbieri2026topvenuestool", PAPER_B),
            "Paper B · using the tool",
            "Artigo B · uso da ferramenta",
        ),
        "{{DEMO}}": DEMO_VIDEO,
        "{{GH}}": GITHUB,
        "{{REPO}}": REPOSITORY_URL,
        "{{HF}}": HUGGING_FACE,
        "{{SITE}}": SITE_URL,
        "{{STAMP}}": f"{PROFILE} · v{release.version} · {release.commit} · {built_on}",
        **release_values(release),
        **quick_start_blocks(release.version),
    }


def render(template: str, values: dict[str, str]) -> str:
    page = template
    for placeholder, value in values.items():
        page = page.replace(placeholder, value)
    unfilled = sorted(set(re.findall(r"\{\{[A-Z_0-9]+\}\}", page)))
    if unfilled:
        raise ValueError(f"unfilled placeholders: {unfilled}")
    return page


def write_site(page: str, repository: Path, out: Path, built_on: str) -> None:
    out.mkdir(parents=True, exist_ok=True)
    (out / "index.html").write_text(page)
    shutil.copytree(SITE_DIR / "assets", out / "assets", dirs_exist_ok=True)
    brand = out / "assets" / "brand"
    brand.mkdir(parents=True, exist_ok=True)
    for name in BRAND_FILES:
        shutil.copy2(repository / "docs" / "brand" / name, brand / name)
    shutil.copytree(
        repository / "docs" / "assets" / "screenshots",
        out / "assets" / "screens",
        dirs_exist_ok=True,
    )
    (out / ".nojekyll").write_text("")
    (out / "robots.txt").write_text(f"User-agent: *\nAllow: /\nSitemap: {SITE_URL}sitemap.xml\n")
    (out / "sitemap.xml").write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        f"<url><loc>{SITE_URL}</loc><lastmod>{built_on}</lastmod></url></urlset>\n"
    )


def build(repository: Path, out: Path) -> Path:
    release = load_release(repository)
    built_on = datetime.date.today().isoformat()
    page = render(
        (SITE_DIR / "template.html").read_text(), page_values(release, repository, built_on)
    )
    page = size_screenshots(page, repository / "docs" / "assets" / "screenshots")
    write_site(page, repository, out, built_on)
    return out / "index.html"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--repository",
        type=Path,
        default=SITE_DIR.parent,
        help="repository root (default: this checkout)",
    )
    arguments = parser.parse_args()
    index = build(arguments.repository.expanduser().resolve(), arguments.out.expanduser().resolve())
    print(f"wrote {index} ({index.stat().st_size / 1024:.0f} KiB)")
