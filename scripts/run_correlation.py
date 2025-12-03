# scripts/run_correlation.py
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import statsmodels.api as sm
from pathlib import Path
import json

def compute_days_from_anniversary(date, month, day):
    year = date.year
    current_anniv = datetime(year, month, day)
    diff = (date - current_anniv).days
    if diff > 182:
        next_anniv = datetime(year + 1, month, day)
        diff = (date - next_anniv).days
    elif diff < -182:
        prev_anniv = datetime(year - 1, month, day)
        diff = (date - prev_anniv).days
    return diff

def run_correlation(parquet_file, anniversary, plot=False):
    df = pd.read_parquet(parquet_file)
    if "date" not in df.columns or "views" not in df.columns:
        raise ValueError("Parquet must contain 'date' and 'views' columns.")

    df["date"] = pd.to_datetime(df["date"])
    try:
        mm, dd = map(int, anniversary.replace("/", "-").split("-"))
    except Exception:
        raise ValueError("Anniversary must be in MM-DD format (e.g., 05-25).")

    df["days_from_anniv"] = df["date"].apply(lambda d: compute_days_from_anniversary(d, mm, dd))
    df["abs_dist"] = df["days_from_anniv"].abs()

    corr_signed = df["views"].corr(df["days_from_anniv"])
    corr_abs = df["views"].corr(df["abs_dist"])

    X = sm.add_constant(df["abs_dist"])
    model = sm.OLS(df["views"], X).fit()

    # --- Prepare output folder ---
    parquet_path = Path(parquet_file)
    event_folder = parquet_path.parent
    event_folder.mkdir(parents=True, exist_ok=True)

    # --- Save summary JSON ---
    summary = {
        "signed_correlation": corr_signed,
        "absolute_correlation": corr_abs,
        "regression": {
            "params": model.params.to_dict(),
            "pvalues": model.pvalues.to_dict(),
            "rsquared": model.rsquared
        }
    }
    json_file = event_folder / f"correlation_{parquet_path.stem}.json"
    with open(json_file, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"Saved correlation summary to {json_file}")

    # --- Optional scatter plot ---
    if plot:
        plt.figure(figsize=(10, 6))
        plt.scatter(df["days_from_anniv"], df["views"], alpha=0.4)
        plt.axvline(0, linestyle="--", color="red", label="Anniversary")
        plt.xlabel("Days From Anniversary")
        plt.ylabel("Daily Pageviews")
        plt.title(f"Pageviews vs Distance From Anniversary ({parquet_path.stem})")
        plt.legend()
        plt.tight_layout()
        png_file = event_folder / f"correlation_plot_{parquet_path.stem}.png"
        plt.savefig(png_file)
        plt.close()
        print(f"Saved correlation plot to {png_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Correlation between pageviews and event anniversary.")
    parser.add_argument("--parquet", required=True, help="Input parquet file containing pageviews")
    parser.add_argument("--anniversary", required=True, help="Anniversary date in MM-DD format (e.g., 01-06)")
    parser.add_argument("--plot", action="store_true", help="Save scatter plot")
    args = parser.parse_args()
    run_correlation(args.parquet, args.anniversary, plot=args.plot)
