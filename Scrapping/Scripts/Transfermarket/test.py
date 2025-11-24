"""Filter a previously scraped Transfermarkt players CSV copy into the minimal
players_from_365.filtered.csv without re-scraping.

Default input: Scrapping/Data/Transfermarkt/Players/players_from_365 copy.csv
Output:       Scrapping/Data/Transfermarkt/Players/players_from_365.filtered.csv

Logic mirrors the post-processing in transfermarkt_players.py:
- Keep core analytical columns
- Drop duplicate (club_id, player_id, player_name) keeping last occurrence
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

KEEP_COLS_ORDER = [
    "league_name","club","club_id","player_name","player_id","player_url",
    "position","age","nationality","market_value","market_value_eur"
]
DEDUP_KEYS = ["club_id","player_id","player_name"]


def filter_players(input_csv: str, output_csv: str) -> None:
    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"Input copy file not found: {input_csv}")

    df = pd.read_csv(input_csv)
    if df.empty:
        print("⚠️ Input file is empty; no output generated.")
        return

    keep_cols = [c for c in KEEP_COLS_ORDER if c in df.columns]
    if not keep_cols:
        raise KeyError("None of the expected columns found in input; ensure this is a raw players scrape copy.")

    # Select + dedupe
    df_min = (
        df[keep_cols]
        .drop_duplicates(subset=[k for k in DEDUP_KEYS if k in df.columns], keep="last")
        .reset_index(drop=True)
    )

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df_min.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"✅ Filtered players saved: {len(df_min)} rows → {output_csv}")
    print(f"   Columns: {', '.join(df_min.columns)}")


def parse_args():
    ap = argparse.ArgumentParser(description="Filter Transfermarkt players copy into minimal dataset")
    ap.add_argument("--in", dest="input_csv", default=DEFAULT_INPUT, help="Path to players_from_365 copy CSV")
    ap.add_argument("--out", dest="output_csv", default=DEFAULT_OUTPUT, help="Path for filtered output CSV")
    return ap.parse_args()


def main():
    args = parse_args()
    filter_players(args.input_csv, args.output_csv)


if __name__ == "__main__":
    main()
