#!/usr/bin/env python3
"""
Convert JSONL.GZ files (from fetch_revisions.py) into Parquet files.
Takes the same pages.txt input file as fetch_revisions.py and converts all available
JSONL.GZ files to Parquet format.

Input:  data/raw/revisions/<title>.jsonl.gz (for each page in pages.txt)
Output: data/cleaned/revisions/<title>.parquet

Usage:
  python scripts/to_parquet.py --pagefile data/input/pages.txt
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
    ap.add_argument("--pagefile", required=True, help="Path to text file containing page titles (one per line)")
    ap.add_argument("--indir", default="data/raw/revisions", help="Input directory containing JSONL.GZ files")
    ap.add_argument("--outdir", default="data/cleaned/revisions", help="Output directory for Parquet files")
    args = ap.parse_args()

    with open(args.pagefile, 'r') as f:
        pages = [line.strip() for line in f if line.strip()]
    
    indir = Path(args.indir)
    outdir = Path(args.outdir)
    
    for page in pages:
        safe = page.replace(" ", "_").replace("/", "_")
        infile = indir / f"{safe}.jsonl.gz"
        outfile = outdir / f"{safe}.parquet"
        
        if not infile.exists():
            print(f"Warning: Input file not found for page '{page}' at {infile}")
            continue
            
        convert_jsonl_to_parquet(infile, outfile)

if __name__ == "__main__":
    main()
