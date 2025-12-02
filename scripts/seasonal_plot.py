# scripts/seasonal_plot.py
import pandas as pd
import matplotlib.pyplot as plt
import argparse

def seasonal_plot(parquet_file, article_name=None, anniversary=None):
    # Load Parquet
    df = pd.read_parquet(parquet_file)

    # Ensure required columns
    if 'date' not in df.columns or 'views' not in df.columns:
        raise ValueError("Parquet must have 'date' and 'views' columns")

    df['date'] = pd.to_datetime(df['date'])

    # Filter by article if specified
    if article_name and 'article' in df.columns:
        article_name_norm = article_name.replace(" ", "_")  # normalize spaces to underscores
        df = df[df['article'] == article_name_norm]

    if df.empty:
        raise ValueError("No data available to plot after filtering")

    # Extract year, month, and day
    df['year'] = df['date'].dt.year
    df['month_day'] = df['date'].dt.strftime('%m-%d')  # MM-DD for plotting
    df['month'] = df['date'].dt.month

    # Pivot table: index = month_day, columns = year, values = views
    pivot = df.pivot_table(index='month_day', columns='year', values='views', aggfunc='sum')
    pivot = pivot.sort_index()

    # Prepare x-axis ticks by month
    month_starts = df.groupby(df['date'].dt.month)['month_day'].min()
    month_labels = [pd.to_datetime(m + "-2020").strftime('%b') for m in month_starts]  # just month names

    # Plot
    plt.figure(figsize=(15,6))
    for year in pivot.columns:
        plt.plot(pivot.index, pivot[year], label=str(year))

    # Add anniversary line if provided
    if anniversary:
        anniversary_norm = anniversary.replace("/", "-")  # allow 01/06 or 01-06
        plt.axvline(anniversary_norm, color='red', linestyle='--', label='Anniversary')

    plt.xlabel("Month")
    plt.ylabel("Daily Pageviews")
    title = f"Daily Wikipedia Pageviews for '{article_name}' by Year" if article_name else "Daily Wikipedia Pageviews by Year"
    plt.title(title)
    
    plt.xticks(ticks=month_starts.values, labels=month_labels, rotation=0)
    plt.grid(True)
    plt.legend(title="Year")
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet", required=True, help="Input Parquet file with pageviews")
    parser.add_argument("--article", help="Filter for a single article (optional)")
    parser.add_argument("--anniversary", help="Optional anniversary day (MM-DD) to draw a vertical line")
    args = parser.parse_args()

    seasonal_plot(args.parquet, article_name=args.article, anniversary=args.anniversary)
