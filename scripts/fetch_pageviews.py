# scripts/fetch_pageviews.py
import requests, pandas as pd, argparse
from datetime import datetime

def fetch_pageviews(article, start, end, project="en.wikipedia", access="all-access", agent="user", granularity="daily"):
    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/{project}/{access}/{agent}/{article}/{granularity}/{start}/{end}"
    r = requests.get(url)
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
    df.to_parquet(args.outfile, index=False)
