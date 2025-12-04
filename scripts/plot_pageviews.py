# scripts/plot_pageviews.py
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import argparse
from pathlib import Path
import datetime

def plot_pageviews(parquet_file, article_name, anniversary=None):

    parquet_path = Path(parquet_file)
    df = pd.read_parquet(parquet_path)

    if "date" not in df.columns or "views" not in df.columns:
        raise ValueError("Parquet must have 'date' and 'views' columns")

    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["doy"] = df["date"].dt.dayofyear


    pivot = df.pivot_table(index="doy", columns="year", values="views", aggfunc="sum")

    plt.figure(figsize=(15,6))
    for year in pivot.columns:
        plt.plot(pivot.index, pivot[year], label=str(year))

    plt.title(f'Yearly Wikipedia Pageviews for "{article_name}"')
    plt.xlabel("Month")
    plt.ylabel("Daily Pageviews")
    plt.grid(True)
    plt.legend(title="Year")

    # Month labels
    xticks = []
    xticklabels = []
    for month in range(1, 13):
        dt = datetime.datetime(2000, month, 1)
        doy = dt.timetuple().tm_yday
        xticks.append(doy)
        xticklabels.append(dt.strftime('%b'))
    plt.xticks(xticks, xticklabels)

    if anniversary:
        try:
            mm, dd = map(int, anniversary.split("-"))
            anniv_doy = datetime.datetime(2000, mm, dd).timetuple().tm_yday
            plt.axvline(anniv_doy, color='red', linestyle='--', label='Anniversary')
        except Exception as e:
            print(f"Invalid anniversary format: {anniversary}. Use MM-DD.")

    plt.tight_layout()

    outfile = parquet_path.parent / f"plotted_{parquet_path.stem}.png"
    plt.savefig(outfile)
    plt.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet", required=True)
    parser.add_argument("--article", required=True)
    parser.add_argument("--anniversary")
    args = parser.parse_args()

    plot_pageviews(args.parquet, article_name = args.article, anniversary = args.anniversary)


