#!/usr/bin/env python3
"""
Minimal Wikipedia revisions fetcher.
Writes newline-delimited JSON (JSONL) compressed with gzip into data/raw/revisions/.
Usage:
  python scripts/fetch_revisions.py --pages "COVID-19 pandemic" --start 2025-01-01T00:00:00Z
"""

import os, json, gzip, time, argparse
from pathlib import Path
import requests

API = "https://en.wikipedia.org/w/api.php"

def fetch_revisions(page_title: str, ua: str, start: str | None, end: str | None, limit: int = 500):
    s = requests.Session()
    s.headers["User-Agent"] = ua
    params = {
        "action": "query",
        "format": "json",
        "prop": "revisions",
        "rvprop": "ids|timestamp|user|comment|content|flags",
        "rvslots": "main",
        "titles": page_title,
        "rvlimit": limit,
        "rvdir": "newer",
    }
    if start: params["rvstart"] = start
    if end:   params["rvend"] = end

    while True:
        r = s.get(API, params=params, timeout=60)
        r.raise_for_status()
        j = r.json()
        pages = j.get("query", {}).get("pages", {})
        for _, p in pages.items():
            for rev in p.get("revisions", []):
                yield {
                    "pageid": p.get("pageid"),
                    "title": p.get("title"),
                    **rev
                }
        if "continue" in j:
            params.update(j["continue"])
            time.sleep(0.1)  # politeness
        else:
            break

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", nargs="+", required=True, help="One or more page titles")
    ap.add_argument("--start", help="ISO timestamp, e.g. 2025-01-01T00:00:00Z")
    ap.add_argument("--end", help="ISO timestamp")
    ap.add_argument("--outdir", default="data/raw/revisions", help="Output directory (JSONL.GZ)")
    args = ap.parse_args()

    ua = os.getenv("WIKI_UA", "Connolly-WikiProject/0.1 (dan.connolly91@gmail.com)")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    for page in args.pages:
        safe = page.replace(" ", "_").replace("/", "_")
        outpath = outdir / f"{safe}.jsonl.gz"
        # append-only so you can re-run safely
        with gzip.open(outpath, "ab") as f:
            for item in fetch_revisions(page, ua, args.start, args.end):
                f.write((json.dumps(item, ensure_ascii=False) + "\n").encode("utf-8"))

if __name__ == "__main__":
    main()
