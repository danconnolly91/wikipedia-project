#!/usr/bin/env python3
import argparse
import pandas as pd
import numpy as np
from datetime import datetime

def parse_args():
    parser = argparse.ArgumentParser(description="Bootstrap CI for anniversary pageview effects")
    parser.add_argument("--parquet", required=True, help="Path to parquet file of pageviews")
    parser.add_argument("--anniversary", required=True, help="MM-DD format (e.g., 09-11)")
    parser.add_argument("--window", type=int, default=0, help="Days on either side of anniversary")
    parser.add_argument("--samples", type=int, default=10000, help="Number of bootstrap samples")
    return parser.parse_args()

def compute_anniversary_mask(df, month, day, window):
    dates = pd.to_datetime(df["date"])
    df["_month"] = dates.dt.month
    df["_day"] = dates.dt.day

    # absolute difference in days-of-year, with wraparound at year boundary
    target = datetime(2000, month, day).timetuple().tm_yday
    doy = dates.dt.dayofyear
    diff = np.abs(doy - target)
    diff = np.minimum(diff, 366 - diff)

    return diff <= window

def bootstrap_ci(a, b, n_samples=10000, ci=0.95):
    nA, nB = len(a), len(b)
    diffs = np.empty(n_samples)

    for i in range(n_samples):
        a_star = np.random.choice(a, nA, replace=True)
        b_star = np.random.choice(b, nB, replace=True)
        diffs[i] = a_star.mean() - b_star.mean()

    lower = np.percentile(diffs, (1-ci)*50)
    upper = np.percentile(diffs, 100 - (1-ci)*50)
    return lower, upper, diffs

def main():
    args = parse_args()

    print("\n===============================")
    print(" Bootstrap CI for Anniversary Effect")
    print("===============================\n")

    df = pd.read_parquet(args.parquet)
    df["date"] = pd.to_datetime(df["date"])

    month, day = map(int, args.anniversary.split("-"))
    mask = compute_anniversary_mask(df, month, day, args.window)

    A = df.loc[mask, "views"].to_numpy()
    B = df.loc[~mask, "views"].to_numpy()

    print(f"Anniversary window: ±{args.window} days around {args.anniversary}")
    print(f"Rows in anniversary window: {len(A)}")
    print(f"Rows in baseline: {len(B)}\n")

    observed = A.mean() - B.mean()
    print(f"Observed difference in mean views:")
    print(f"  anniversary - baseline = {observed:.2f}\n")

    print("Bootstrapping...")
    lower, upper, diffs = bootstrap_ci(A, B, args.samples)

    print("\n95% bootstrap confidence interval:")
    print(f"  [{lower:.2f}, {upper:.2f}]\n")


if __name__ == "__main__":
    main()
