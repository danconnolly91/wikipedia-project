# scripts/plot_pageviews.py
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import argparse
from pathlib import Path

def plot_pageviews(parquet_file, article_name=None):
    # Load data
    df = pd.read_parquet(parquet_file)

    if 'date' not in df.columns or 'views' not in df.columns:
        raise ValueError("Parquet must have 'date' and 'views' columns")

    df['date'] = pd.to_datetime(df['date'])

    # Filter by article if specified
    if article_name and 'article' in df.columns:
        df = df[df['article'] == article_name]

    if df.empty:
        raise ValueError("No data available to plot after filtering")

    # Extract year and day-of-year
    df['year'] = df['date'].dt.year
    df['doy'] = df['date'].dt.dayofyear

    # Pivot table: rows = day-of-year, columns = year, values = views
    pivot = df.pivot_table(index='doy', columns='year', values='views', aggfunc='sum')

    # Plot each year as a separate line
    plt.figure(figsize=(15,6))
    for year in pivot.columns:
        plt.plot(pivot.index, pivot[year], label=str(year))

    # Set labels and title
    if article_name is None and 'article' in df.columns:
        article_name = df['article'].iloc[0].replace("_", " ")

    title = f"Yearly Wikipedia Pageviews for \"{article_name}\""
    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel("Daily Pageviews")
    plt.grid(True)

    # Add legend
    if len(pivot.columns) > 0:
        plt.legend(title="Year")

    # Adjust X-axis to show months instead of day-of-year
    # Create mapping of day-of-year to approximate date for labeling
    import datetime
    start_date = datetime.datetime(2000, 1, 1)  # arbitrary leap year to handle 366 days
    xticks = []
    xticklabels = []
    for month in range(1,13):
        dt = datetime.datetime(2000, month, 1)
        doy = dt.timetuple().tm_yday
        xticks.append(doy)
        xticklabels.append(dt.strftime('%b'))
    plt.xticks(xticks, xticklabels)

    plt.tight_layout()

    # Automatically save PNG in same folder as parquet
    parquet_path = Path(parquet_file)
    png_file = parquet_path.parent / f"plotted_{parquet_path.stem}.png"
    plt.savefig(png_file)
    plt.close()
    print(f"Saved plot to {png_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet", required=True, help="Input Parquet file with pageviews")
    parser.add_argument("--article", help="Filter for a single article (optional)")
    args = parser.parse_args()

    plot_pageviews(args.parquet, article_name=args.article)

