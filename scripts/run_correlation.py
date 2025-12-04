# scripts/correlate_anniversary.py
import argparse
import pandas as pd
import statsmodels.api as sm
from datetime import datetime
from pathlib import Path


def compute_days_from_anniversary(date, month, day):
    year = date.year
    current = datetime(year, month, day)
    diff = (date - current).days

    if diff > 182:
        diff = (date - datetime(year+1, month, day)).days
    elif diff < -182:
        diff = (date - datetime(year-1, month, day)).days

    return diff


def run_correlation(parquet_file, anniversary, outfile):

    df = pd.read_parquet(parquet_file)
    df["date"] = pd.to_datetime(df["date"])

    mm, dd = map(int, anniversary.split("-"))

    df["days_from_anniv"] = df["date"].apply(lambda d: compute_days_from_anniversary(d, mm, dd))
    df["abs_dist"] = df["days_from_anniv"].abs()

    corr_signed = df["views"].corr(df["days_from_anniv"])
    corr_abs = df["views"].corr(df["abs_dist"])

    # Regression
    X = sm.add_constant(df["abs_dist"])
    model = sm.OLS(df["views"], X).fit()

    # Write results
    with open(outfile, "w") as f:
        f.write(f"Signed correlation: {corr_signed:.4f}\n")
        f.write(f"Absolute correlation: {corr_abs:.4f}\n\n")
        f.write(model.summary().as_text())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet", required=True)
    parser.add_argument("--anniversary", required=True)
    parser.add_argument("--outfile", required=True)
    args = parser.parse_args()

    run_correlation(args.parquet, args.anniversary, args.outfile)