# scripts/test_anniversary_permutation.py

import argparse
import numpy as np
import pandas as pd
from datetime import datetime

def permutation_test_anniversary(df, anniversary, window=3, n_permutations=10000, seed=0):
    np.random.seed(seed)

    # Extract month/day
    anniv_month, anniv_day = map(int, anniversary.split("-"))

    # Compute days_from_anniv for each year
    df = df.copy()
    df["anniv_date"] = df["date"].apply(lambda d: d.replace(month=anniv_month, day=anniv_day))
    df["days_from_anniv"] = (df["date"] - df["anniv_date"]).dt.days

    # Anniversary window mask
    anniv_mask = df["days_from_anniv"].abs() <= window

    anniversary_views = df.loc[anniv_mask, "views"].values
    baseline_views = df.loc[~anniv_mask, "views"].values

    observed = anniversary_views.mean() - baseline_views.mean()

    # Permutation test
    combined = df["views"].values.copy()
    perm_diffs = []

    for _ in range(n_permutations):
        np.random.shuffle(combined)
        perm_anniv = combined[:len(anniversary_views)]
        perm_base = combined[len(anniversary_views):]
        perm_diffs.append(perm_anniv.mean() - perm_base.mean())

    perm_diffs = np.array(perm_diffs)
    p_value = np.mean(np.abs(perm_diffs) >= np.abs(observed))

    return observed, p_value, len(anniversary_views), len(baseline_views)


def main():
    parser = argparse.ArgumentParser(description="Permutation Test for Anniversary Pageview Boost")
    parser.add_argument("--parquet", required=True, help="Input parquet file from fetch_pageviews.py")
    parser.add_argument("--anniversary", required=True, help="Anniversary date MM-DD")
    parser.add_argument("--window", type=int, default=3, help="Window size in days (default: ±3)")
    parser.add_argument("--permutations", type=int, default=10000, help="Number of permutations")

    args = parser.parse_args()

    # Load data
    df = pd.read_parquet(args.parquet)
    df["date"] = pd.to_datetime(df["date"])

    observed, p_value, n_anniv, n_base = permutation_test_anniversary(
        df,
        anniversary=args.anniversary,
        window=args.window,
        n_permutations=args.permutations
    )

    print("\n===============================")
    print(" Permutation Test Results")
    print("===============================\n")

    print(f"Anniversary window: ±{args.window} days around {args.anniversary}")
    print(f"Rows in anniversary window: {n_anniv}")
    print(f"Rows in baseline: {n_base}\n")

    print(f"Observed difference in mean views:")
    print(f"  anniversary - baseline = {observed:.2f}\n")

    print(f"Permutation p-value: {p_value:.5f}")

    if p_value < 0.05:
        print("\n🎉 Significant anniversary effect detected!")
    else:
        print("\nNo statistically significant anniversary effect detected.")

    print("\n")


if __name__ == "__main__":
    main()

