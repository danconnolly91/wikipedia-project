#!/usr/bin/env python3
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import statsmodels.api as sm
import numpy as np


def compute_days_from_anniversary(date, month, day):
    """
    Convert any date into its signed distance from the nearest anniversary.
    This creates a circular 365-day variable where 0 = anniversary.
    """
    year = date.year
    current_anniv = datetime(year, month, day)
    diff = (date - current_anniv).days

    # Wrap-around handling for circular year
    if diff > 182:
        next_anniv = datetime(year + 1, month, day)
        diff = (date - next_anniv).days
    elif diff < -182:
        prev_anniv = datetime(year - 1, month, day)
        diff = (date - prev_anniv).days

    return diff


def run_correlation(parquet_file, anniversary, plot=False):

    # --- Load data ---
    df = pd.read_parquet(parquet_file)
    if "date" not in df.columns or "views" not in df.columns:
        raise ValueError("Parquet must contain 'date' and 'views' columns.")

    df["date"] = pd.to_datetime(df["date"])

    # --- Parse anniversary ---
    try:
        mm, dd = map(int, anniversary.replace("/", "-").split("-"))
    except Exception:
        raise ValueError("Anniversary must be in MM-DD format (e.g., 05-25).")

    # --- Compute distance from anniversary ---
    df["days_from_anniv"] = df["date"].apply(lambda d: compute_days_from_anniversary(d, mm, dd))
    df["abs_dist"] = df["days_from_anniv"].abs()

    # --- Correlations ---
    corr_signed = df["views"].corr(df["days_from_anniv"])
    corr_abs = df["views"].corr(df["abs_dist"])

    print("\n===============================")
    print(" Correlation Results")
    print("===============================")
    print(f"Signed distance correlation (views ~ days_from_anniv): {corr_signed:.4f}")
    print(f"Absolute distance correlation (views ~ |days_from_anniv|): {corr_abs:.4f}\n")

    # --- Regression (views ~ absolute distance) ---
    X = sm.add_constant(df["abs_dist"])
    model = sm.OLS(df["views"], X).fit()

    print(model.summary())

    # --- Optional scatter plot ---
    if plot:
        plt.figure(figsize=(10, 6))
        plt.scatter(df["days_from_anniv"], df["views"], alpha=0.4)
        plt.axvline(0, linestyle="--", color="red", label="Anniversary")
        plt.xlabel("Days From Anniversary")
        plt.ylabel("Daily Pageviews")
        plt.title("Pageviews vs Distance From Anniversary")
        plt.legend()
        plt.tight_layout()
        plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Correlation between pageviews and event anniversary.")
    parser.add_argument("--parquet", required=True, help="Input parquet file containing pageviews")
    parser.add_argument("--anniversary", required=True, help="Anniversary date in MM-DD format (e.g., 01-06)")
    parser.add_argument("--plot", action="store_true", help="Show scatter plot")

    args = parser.parse_args()

    run_correlation(args.parquet, args.anniversary, plot=args.plot)
