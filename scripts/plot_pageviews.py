# scripts/plot_pageviews.py
import pandas as pd
import matplotlib.pyplot as plt
import argparse

def plot_pageviews(parquet_file, article_name=None):
    # Load the data
    df = pd.read_parquet(parquet_file)

    # Make sure required columns exist
    if 'date' not in df.columns or 'views' not in df.columns:
        raise ValueError("Parquet must have 'date' and 'views' columns")

    df['date'] = pd.to_datetime(df['date'])

    # Filter by article if specified
    if article_name:
        if 'article' not in df.columns:
            raise ValueError("Parquet does not have 'article' column to filter")
        df = df[df['article'] == article_name]

    if df.empty:
        raise ValueError("No data available to plot after filtering")

    # Extract year and day-of-year
    df['year'] = df['date'].dt.year
    df['doy'] = df['date'].dt.dayofyear

    # Pivot table: rows = day-of-year, columns = year, values = views
    pivot = df.pivot_table(index='doy', columns='year', values='views', aggfunc='sum')

    # Plot
    plt.figure(figsize=(15,6))
    for year in pivot.columns:
        plt.plot(pivot.index, pivot[year], label=str(year))

    plt.xlabel("Day of Year")
    plt.ylabel("Daily Pageviews")
    title = f"Daily Pageviews for {article_name}" if article_name else "Daily Pageviews"
    plt.title(title)
    plt.grid(True)

    # Add legend if multiple years
    if len(pivot.columns) > 0:
        plt.legend(title="Year")

    plt.tight_layout()
    plt.show()  # <-- just show the plot

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet", required=True, help="Input Parquet file with pageviews")
    parser.add_argument("--article", help="Filter for a single article (optional)")
    args = parser.parse_args()

    plot_pageviews(args.parquet, article_name=args.article)
