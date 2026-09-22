"""Tests for evidence-backed bibliographic deduplication."""

from src.deduplication import canonical_resource_locator, deduplicate_papers
from src.models import Paper


def _paper(paper_id: str, ee: str, **updates: object) -> Paper:
    values = {
        "paper_id": paper_id,
        "title": "A paper",
        "year": 2025,
        "event": "ACM CCS",
        "ee": ee,
    }
    values.update(updates)
    return Paper(**values)


def test_canonical_resource_locator_normalizes_doi_url_variants() -> None:
    assert (
        canonical_resource_locator("https://doi.org/10.1145/Example?source=foo")
        == "doi:10.1145/example"
    )
    assert canonical_resource_locator("10.1145/EXAMPLE") == "doi:10.1145/example"


def test_exact_resource_duplicates_merge_and_keep_dblp_key() -> None:
    papers, report = deduplicate_papers(
        [
            _paper("91742", "https://doi.org/10.1145/Example", abstract="short"),
            _paper(
                "conf/ccs/Example25",
                "https://doi.org/10.1145/example",
                abstract="a longer abstract retained after the merge",
                bibtex="@inproceedings{example, title={A paper}}",
            ),
        ]
    )

    assert report.input_records == 2
    assert report.output_records == 1
    assert report.merged_records == 1
    assert papers[0].paper_id == "conf/ccs/Example25"
    assert papers[0].abstract == "a longer abstract retained after the merge"


def test_similar_titles_without_shared_resource_are_not_merged() -> None:
    papers, report = deduplicate_papers(
        [
            _paper("conf/ccs/One25", "https://doi.org/10.1145/one"),
            _paper("journals/csur/Two25", "https://doi.org/10.1145/two"),
        ]
    )

    assert report.merged_records == 0
    assert len(papers) == 2


def test_merge_prefers_complete_abstract_over_longer_truncated_text() -> None:
    complete = (
        "We evaluate the proposed security method using controlled experiments and report "
        "evidence that supports the complete conclusion."
    )
    truncated = (
        "We evaluate the proposed security method using controlled experiments, multiple "
        "datasets, extensive baselines, practitioner feedback, and detailed evidence that "
        "continues into a missing final portion..."
    )
    papers, _ = deduplicate_papers(
        [
            _paper("legacy", "https://doi.org/10.1145/example", abstract=truncated),
            _paper("conf/ccs/Example25", "10.1145/example", abstract=complete),
        ]
    )

    assert papers[0].abstract == complete
