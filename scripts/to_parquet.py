#!/usr/bin/env python3
"""
Convert a JSONL.GZ file (from fetch_revisions.py) into a Parquet file.
Input:  data/raw/revisions/<title>.jsonl.gz
Output: data/cleaned/revisions/<title>.parquet
Usage:
  python scripts/to_parquet.py \
    --infile data/raw/revisions/COVID-19_pandemic.jsonl.gz \
    --outfile data/cleaned/revisions/COVID-19_pandemic.parquet
"""
import gzip, json, argparse
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

def convert_jsonl_to_parquet(infile: Path, outfile: Path):
    rows = []
    with gzip.open(infile, "rt", encoding="utf-8") as f:
        for line in f:
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            rows.append({
                "pageid": r.get("pageid"),
                "title": r.get("title"),
                "revid": r.get("revid"),
                "parentid": r.get("parentid"),
                "timestamp": r.get("timestamp"),
                "user": r.get("user"),
                "comment": r.get("comment"),
                "minor": "minor" in r,
                "wikitext": (
                    r.get("slots", {}).get("main", {}).get("*")
                    if isinstance(r.get("slots"), dict) else None
                ),
            })
    table = pa.Table.from_pylist(rows)
    outfile.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, outfile, compression="zstd")
    print(f"Wrote {len(rows)} rows → {outfile}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--outfile", required=True)
    args = ap.parse_args()
    convert_jsonl_to_parquet(Path(args.infile), Path(args.outfile))

if __name__ == "__main__":
    main()
