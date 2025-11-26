"""Filter a previously scraped Transfermarkt players CSV copy by removing
duplicates only, while keeping ALL attributes/columns intact.

Default input: Scrapping/Data/Transfermarkt/Players/players_from_365 copy.csv
Output:       Scrapping/Data/Transfermarkt/Players/players_from_365.filtered.csv

Rules:
- Only drop duplicates; do not drop columns.
- Keep the FIRST occurrence when duplicates are found.
- By default, duplicates are identified by (club_id, player_id, player_name).
    If any of these keys are missing, fall back to exact-row duplicates.
"""
from __future__ import annotations
import os
import argparse
import pandas as pd

DEFAULT_INPUT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "Data", "Transfermarkt", "Players", "players_from_365 copy.csv"
)
DEFAULT_OUTPUT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "Data", "Transfermarkt", "Players", "players_from_365.filtered.csv"
)

DEFAULT_DEDUP_KEYS = ["club_id","player_id","player_name"]


def filter_players(input_csv: str, output_csv: str, dedup_keys: list[str] | None = None) -> None:
    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"Input copy file not found: {input_csv}")

    df = pd.read_csv(input_csv)
    if df.empty:
        print("⚠️ Input file is empty; no output generated.")
        return

    # Determine dedup keys
    subset_keys: list[str] | None = None
    if dedup_keys:
        subset_keys = [k for k in dedup_keys if k in df.columns]
        if not subset_keys:
            subset_keys = None  # fall back to exact-row duplicates
    else:
        subset_keys = [k for k in DEFAULT_DEDUP_KEYS if k in df.columns]
        if not subset_keys:
            subset_keys = None

    # Only drop duplicates; keep all columns, keep FIRST occurrence
    df_out = df.drop_duplicates(subset=subset_keys, keep="first").reset_index(drop=True)

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df_out.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"✅ Filtered players saved: {len(df_out)} rows → {output_csv}")
    print(f"   Columns kept: {len(df_out.columns)} → {', '.join(df_out.columns)}")
    if subset_keys:
        print(f"   Dedup subset: {subset_keys} (keep='first')")
    else:
        print("   Dedup subset: full row (exact duplicates), keep='first'")


def parse_args():
    ap = argparse.ArgumentParser(description="Filter Transfermarkt players copy by removing duplicates only (keep all columns)")
    ap.add_argument("--in", dest="input_csv", default=DEFAULT_INPUT, help="Path to players_from_365 copy CSV")
    ap.add_argument("--out", dest="output_csv", default=DEFAULT_OUTPUT, help="Path for filtered output CSV")
    ap.add_argument(
        "--subset", nargs="*", default=DEFAULT_DEDUP_KEYS,
        help="Columns to identify duplicates (default: club_id player_id player_name). If none of these exist, falls back to exact-row duplicates."
    )
    return ap.parse_args()


def main():
    args = parse_args()
    filter_players(args.input_csv, args.output_csv, dedup_keys=args.subset)


if __name__ == "__main__":
    main()
