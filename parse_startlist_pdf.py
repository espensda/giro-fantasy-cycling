"""Convert a PCS startlist PDF into bootstrap JSON."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from scraper import GiroScraper


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert a PCS startlist PDF into JSON.")
    parser.add_argument("pdf_path", help="Path to the PCS startlist PDF")
    parser.add_argument("year", type=int, help="Season year for the output file")
    parser.add_argument(
        "--output",
        help="Optional output path. Defaults to data/bootstrap_startlist/giro_<year>.json",
    )
    args = parser.parse_args()

    pdf_path = Path(args.pdf_path)
    if not pdf_path.exists() or not pdf_path.is_file():
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")

    output_path = Path(args.output) if args.output else Path("data/bootstrap_startlist") / f"giro_{args.year}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows = GiroScraper().parse_startlist_pdf_bytes(pdf_path.read_bytes())
    output_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    rider_count = len([row for row in rows if row["category"] != "ds"])
    ds_count = len([row for row in rows if row["category"] == "ds"])
    team_count = len({row["team"] for row in rows if row["category"] != "ds"})

    print(f"Saved {len(rows)} rows to {output_path}")
    print(f"Teams: {team_count}")
    print(f"Riders: {rider_count}")
    print(f"DS teams: {ds_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())