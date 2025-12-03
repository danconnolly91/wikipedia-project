# scripts/fetch_pageviews.py
import requests, pandas as pd, argparse
from datetime import datetime
from pathlib import Path

def fetch_pageviews(article, start, end, project="en.wikipedia", access="all-access", agent="user", granularity="daily"):
    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/{project}/{access}/{agent}/{article}/{granularity}/{start}/{end}"
    headers = {
        "User-Agent": "Connolly-WikiProject/0.1 (dan.connolly91@gmail.com)"
    }
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    items = r.json()["items"]
    df = pd.DataFrame([{
        "article": x["article"],
        "date": x["timestamp"][:8],
        "views": x["views"]
    } for x in items])
    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--article", required=True)
    parser.add_argument("--start", default="20150101")
    parser.add_argument("--end", default=datetime.now().strftime("%Y%m%d"))
    parser.add_argument("--outfile", required=True)
    args = parser.parse_args()

    df = fetch_pageviews(args.article, args.start, args.end)
    out_path = Path(args.outfile)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.outfile, index=False)
