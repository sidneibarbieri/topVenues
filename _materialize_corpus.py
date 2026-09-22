"""Materialize DBLP listings for the newly-wired venues from the local dump.

This driver avoids the rate-limited live DBLP TOC API.  It parses
data/dblp/dblp.xml.gz once and emits the per-event JSON files consumed by the
existing consolidator.
"""

from src.collector import Collector
from src.dblp_dump_materializer import DblpDumpMaterializer

NEW_VENUES = [
    "esorics",
    "codaspy",
    "raid",
    "cns",
    "wisec",
    "woot",
    "satml",
    "aisec",
    "trustcom",
    "sigcomm",
    "nsdi",
    "imc",
    "sigmetrics",
    "atc",
    "eurosys",
    "mobicom",
    "mobisys",
    "sensys",
    "hotmobile",
    "neurips",
    "icml",
    "iclr",
    "aaai",
    "ijcai",
    "kdd",
    "acl",
    "emnlp",
    "naacl",
]


def main() -> None:
    collector = Collector(base_dir=".")
    years = collector.config.effective_years()
    print(
        f"Materializing {len(NEW_VENUES)} venues x {len(years)} years "
        f"({years[0]}-{years[-1]}) from local DBLP dump",
        flush=True,
    )
    entries = DblpDumpMaterializer(
        config=collector.config,
        json_dir=collector.json_dir,
        log_dir=collector.log_dir,
        dump_path=collector.base_dir / "data" / "dblp" / "dblp.xml.gz",
    ).materialize(events=NEW_VENUES, years=years, overwrite=False)
    downloaded = sum(1 for entry in entries if entry.status.value == "downloaded")
    valid = sum(1 for entry in entries if entry.status.value == "valid")
    failed = sum(1 for entry in entries if entry.status.value == "failed")
    print(
        f"Listing materialization complete: {downloaded} downloaded, "
        f"{valid} already valid, {failed} failed. "
        "See data/log/download_log.csv",
        flush=True,
    )


if __name__ == "__main__":
    main()
