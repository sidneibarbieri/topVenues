"""Tests for the abstract quality gate and title normalization."""

from src.abstract_quality import (
    looks_like_abstract,
    normalize_abstract_text,
    select_best_abstract,
)
from src.open_abstract_sources import _normalize_title, source_for


class TestLooksLikeAbstract:
    def test_rejects_none_and_short(self):
        assert not looks_like_abstract(None)
        assert not looks_like_abstract("")
        assert not looks_like_abstract("Too short.")

    def test_rejects_author_list_with_proceedings(self):
        text = (
            "Elita Lobo, Chirag Agarwal, Himabindu Lakkaraju. Proceedings "
            "of the 2024 Conference of the North American Chapter."
        )
        assert not looks_like_abstract(text)

    def test_rejects_author_list_with_single_initial(self):
        text = (
            "Sonal Kumar, Sreyan Ghosh, S Sakshi, Utkarsh Tyagi, Dinesh "
            "Manocha. Proceedings of the 2024 Conference."
        )
        assert not looks_like_abstract(text)

    def test_rejects_all_caps_title(self):
        assert not looks_like_abstract("AN ALL CAPS TITLE ECHOED BACK " * 4)

    def test_accepts_real_abstract(self):
        text = (
            "Over the past two decades, time series motif discovery has "
            "become a critical task in data mining across many domains."
        )
        assert looks_like_abstract(text)


class TestAbstractNormalization:
    def test_preserves_source_paragraph_boundaries(self):
        source = (
            "<jats:p>First paragraph with useful prose.</jats:p><jats:p>Second paragraph.</jats:p>"
        )

        assert normalize_abstract_text(source) == (
            "First paragraph with useful prose.\n\nSecond paragraph."
        )

    def test_collapses_hard_wrapped_lines_inside_a_paragraph(self):
        source = "First line of one paragraph.\nSecond line of the same paragraph."

        assert normalize_abstract_text(source) == (
            "First line of one paragraph. Second line of the same paragraph."
        )

    def test_complete_candidate_beats_a_longer_truncated_candidate(self):
        complete = (
            "We present a reproducible security analysis with controlled experiments, "
            "transparent measurements, and evidence that supports the reported conclusion."
        )
        truncated = (
            "We present a substantially longer security analysis with controlled experiments, "
            "transparent measurements, extensive ablations, multiple datasets, additional "
            "baselines, practitioner interviews, and detailed evidence that suddenly ends..."
        )

        assert select_best_abstract([truncated, complete]) == complete


class TestSourceRouting:
    def test_routes_by_host(self):
        assert source_for("https://openreview.net/forum?id=abc").__name__ == "fetch_openreview"
        assert source_for("http://proceedings.mlr.press/v139/x.html").__name__ == "fetch_pmlr"
        assert (
            source_for("https://proceedings.neurips.cc/paper/x-Abstract.html").__name__
            == "fetch_neurips"
        )
        assert source_for("https://papers.nips.cc/paper/x").__name__ == "fetch_neurips"
        assert source_for("https://doi.org/10.1145/1234") is None
        assert source_for(None) is None

    def test_title_normalization_ignores_punctuation_and_case(self):
        assert _normalize_title("Learning to Induce Causal Structure.") == _normalize_title(
            "learning to induce causal structure"
        )
