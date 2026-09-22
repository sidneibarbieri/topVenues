from src.preprints import arxiv_api_url, parse_arxiv_atom

ATOM = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <id>http://arxiv.org/abs/2601.12345v1</id>
    <updated>2026-01-03T00:00:00Z</updated>
    <published>2026-01-02T00:00:00Z</published>
    <title>  A security\n preprint </title>
    <summary> Evidence from a system. </summary>
    <author><name>Alice Smith</name></author>
    <arxiv:doi>10.1000/example</arxiv:doi>
  </entry>
</feed>"""


def test_arxiv_parser_keeps_name_match_status_explicit() -> None:
    candidate = parse_arxiv_atom(ATOM, "Alice Smith")[0]
    assert candidate.arxiv_id == "2601.12345v1"
    assert candidate.title == "A security preprint"
    assert candidate.identity_status == "name-match candidate"
    assert candidate.doi == "10.1000/example"


def test_arxiv_api_query_is_encoded() -> None:
    url = arxiv_api_url("Alice Smith", max_results=5)
    assert "max_results=5" in url
    assert "Alice+Smith" in url
