"""Lightweight cleaner for SoFIFA raw player stats (player_stats_raw.csv).
Provides one-hot expansion for multivalue columns, date parts, basic numeric fixes,
optionally drops sparse/low-variance & noisy columns.

Use instead of or in addition to the full standardized cleaner when you want
an aggressively trimmed analytical dataset.
"""

import argparse
import os
import sys
import pandas as pd


def clean_country_kit_number(df: pd.DataFrame, col: str = "country_kit_number") -> pd.DataFrame:
    if col not in df.columns:
        return df
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df


def add_dob_parts(df: pd.DataFrame, dob_col: str = "dob") -> pd.DataFrame:
    if dob_col not in df.columns:
        return df
    df[dob_col] = pd.to_datetime(df[dob_col], errors="coerce")
    df["yearob"] = df[dob_col].dt.year
    df["monthob"] = df[dob_col].dt.month
    return df


def add_date_parts(df: pd.DataFrame, col: str = "club_joined") -> pd.DataFrame:
    if col not in df.columns:
        return df
    df[col] = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
    df["year_club_joined"] = df[col].dt.year
    df["month_club_joined"] = df[col].dt.month
    return df


def expand_multivalue(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col not in df.columns:
        return df
    df[col] = df[col].fillna("").astype(str)
    if not df[col].str.contains(",").any() and df[col].nunique() < 5:
        # Skip expansion if column isn't really multivalued
        return df
    dummies = df[col].str.get_dummies(sep=",").rename(columns=lambda x: x.strip())
    return pd.concat([df, dummies], axis=1)


def drop_sparse_and_low_variance(df: pd.DataFrame, sparse_threshold: float = 0.5) -> pd.DataFrame:
    # Drop completely empty
    df = df.dropna(axis=1, how="all")
    # Drop columns with a single unique (including NaN filtering)
    df = df.loc[:, df.nunique(dropna=False) > 1]
    # Drop columns with > sparse_threshold fraction missing
    min_non_na = int((1 - sparse_threshold) * len(df))
    df = df.dropna(axis=1, thresh=min_non_na)
    return df


DEFAULT_DROP = {
    # Textual / bulky / often not needed for lightweight ML feature sets
    "description", "image", "url", "real_face", "release_clause",
    # Redundant IDs (keep player_id, club_id, country_id if present elsewhere)
    "club_logo", "country_flag", "handedness",
}

ATTRIBUTE_PREFIXES = [
    "attacking_", "skill_", "movement_", "power_", "mentality_", "defending_", "goalkeeping_"
]


def clean_players_raw(input_csv: str, output_csv: str, aggressive: bool = True) -> None:
    if not os.path.exists(input_csv):
        print(f"❌ Input file not found: {input_csv}")
        return
    df = pd.read_csv(input_csv)
    start_cols = set(df.columns)

    # Basic transforms
    df = clean_country_kit_number(df)
    df = add_dob_parts(df)
    df = add_date_parts(df)
    for multi in ["positions", "specialities", "play_styles"]:
        df = expand_multivalue(df, multi)

    if aggressive:
        df = drop_sparse_and_low_variance(df)
        # Drop explicit noisy columns if present
        to_drop = [c for c in DEFAULT_DROP if c in df.columns]
        if to_drop:
            df = df.drop(columns=to_drop)
            print(f"🗑️ Dropped explicit columns: {', '.join(to_drop)}")

    # Coerce key numeric simple fields
    for num_col in ["overall_rating", "potential", "value", "wage"]:
        if num_col in df.columns:
            df[num_col] = pd.to_numeric(df[num_col], errors="coerce")

    # Coerce attribute prefixes
    for col in list(df.columns):
        if any(col.startswith(p) for p in ATTRIBUTE_PREFIXES):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Save
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df.to_csv(output_csv, index=False)

    removed = start_cols - set(df.columns)
    print(f"✅ Cleaned players raw -> {output_csv} (rows={len(df)}, cols={len(df.columns)})")
    if removed:
        print(f"   Removed {len(removed)} columns (sparse/drop rules)")


def parse_args():
    ap = argparse.ArgumentParser(description="Lightweight cleaner for player_stats_raw.csv")
    ap.add_argument("--in", dest="input_csv", default=os.path.join("Scrapping", "Data", "soFIFA", "Players", "player_stats_raw.csv"))
    ap.add_argument("--out", dest="output_csv", default=os.path.join("Scrapping", "Data", "soFIFA", "Players", "player_stats_cleaned_light.csv"))
    ap.add_argument("--no-aggressive", action="store_true", help="Disable dropping sparse & low-variance columns")
    return ap.parse_args()


def main():
    args = parse_args()
    clean_players_raw(args.input_csv, args.output_csv, aggressive=not args.no_aggressive)


if __name__ == "__main__":
    main()
