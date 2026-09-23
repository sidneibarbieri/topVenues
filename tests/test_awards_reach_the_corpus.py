"""The award tables must reach the corpus that is actually served.

The join is keyed on a directory path. Once profile databases moved under
``data/workspaces/<profile>/dataset/``, the callers that walked up from the
database file resolved to a directory that does not exist — and a missing
directory reads as "this corpus has no awards". The Award column went blank and
the awarded-only filter returned nothing, with no error anywhere.

The unit tests over synthetic corpora could not see it, because they pass the
directory in. These check the real directory, the real profile, and that no
caller goes back to deriving the path from the database file.
"""

from __future__ import annotations

from src.awards import awards_directory, build_corpus_award_map
from src.profiles import PROJECT_ROOT, select_profile_id, verified_profile_snapshot
from tests.repository_only import skip_unless_repository

skip_unless_repository()

DERIVED_FROM_DATABASE = 'parent.parent / "awards"'


def test_the_award_tables_are_where_the_resolver_looks() -> None:
    directory = awards_directory()
    assert directory.exists(), directory
    assert list(directory.glob("*_paper_awards.json"))


def test_the_active_profile_resolves_its_awards() -> None:
    """A release ships award labels; an empty map means the join broke again."""
    with verified_profile_snapshot(select_profile_id()) as verified:
        award_map = build_corpus_award_map(awards_directory(), verified.database_path)
    assert award_map, "no paper in the active profile resolved an award"


def test_no_caller_derives_the_award_directory_from_the_database_file() -> None:
    callers = [PROJECT_ROOT / "web" / "app.py", *(PROJECT_ROOT / "src").glob("*.py")]
    offenders = [
        path.relative_to(PROJECT_ROOT)
        for path in callers
        if DERIVED_FROM_DATABASE in path.read_text(encoding="utf-8")
    ]
    assert not offenders, f"use awards_directory() instead: {offenders}"
