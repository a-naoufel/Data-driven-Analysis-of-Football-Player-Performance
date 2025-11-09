import argparse
import os
import re
import unicodedata
import pandas as pd
import numpy as np


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _fix_numeric_name(row: pd.Series) -> str:
    name = f"{row.get('name', '')}".strip()
    if name and not name.isdigit():
        return name
    club_id = str(row.get("club_id", "")).strip()
    patch = {
        # example patch mapping
        "132681": "United Tigers SC",
    }
    if club_id in patch:
        return patch[club_id]
    url = f"{row.get('url', '')}".strip()
    m = re.search(r"/team/\d+/([^/?#]+)/?", url)
    if m:
        slug = m.group(1)
        guess = _norm_space(slug.replace("-", " "))
        return guess.title()
    return name


def _normalize_league(row: pd.Series) -> str:
    league = f"{row.get('league', '')}".strip()
    country = f"{row.get('country', '')}".strip()
    canon = {
        "Serie A": "Serie A",
        "Serie B": "Serie B",
        "Premier League": "Premier League",
        "Bundesliga": "Bundesliga",
        "La Liga": "La Liga",
        "Ligue 1": "Ligue 1 Uber Eats",
        "Ligue 2": "Ligue 2 BKT",
        "Eredivisie": "Eredivisie",
        "EFL Championship": "EFL Championship",
        "EFL League One": "EFL League One",
        "EFL League Two": "EFL League Two",
        "Scottish Premiership": "Scottish Premiership",
    }
    if league in canon:
        league = canon[league]
    if league == "Pro League":
        if country == "Belgium":
            return "Belgian Pro League"
        if country in ("Saudi Arabia", "Saudi"):
            return "Saudi Pro League"
        if country in ("United Arab Emirates", "UAE"):
            return "UAE Pro League"
    if league == "Super League":
        if country == "Greece":
            return "Greek Super League"
        if country == "India":
            return "Indian Super League"
        if country == "Switzerland":
            return "Swiss Super League"
        if country == "China":
            return "Chinese Super League"
    return league


def _parse_club_worth(value):
    if isinstance(value, str):
        v = value.replace("€", "").replace(",", "").strip().upper()
        try:
            if v.endswith("B"):
                return float(v[:-1]) * 1000.0
            if v.endswith("M"):
                return float(v[:-1])
            if v.endswith("K"):
                return float(v[:-1]) / 1000.0
            if v:
                return float(v) / 1_000_000.0
        except ValueError:
            return np.nan
    return np.nan


def clean_sofifa_clubs(input_csv: str, output_csv: str) -> None:
    df = pd.read_csv(input_csv)

    for col in ["name", "league", "country", "stadium", "manager", "rival_team", "url"]:
        if col in df.columns:
            df[col] = df[col].astype(str).map(_norm_space)

    if "name" in df.columns:
        df["name"] = df.apply(_fix_numeric_name, axis=1)

    if "league" in df.columns:
        df["league"] = df.apply(_normalize_league, axis=1)

    if "club_worth" in df.columns:
        df["club_worth_million"] = df["club_worth"].apply(_parse_club_worth)

    cols_order = [
        "club_id", "name", "league", "country",
        "rating", "attack_rating", "midfield_rating", "defense_rating",
        "club_worth", "club_worth_million",
        "stadium", "manager", "manager_id", "manager_url",
        "starting_xi_avg_age", "whole_team_avg_age",
        "rival_team", "players_count", "top_players",
        "club_logo", "country_flag", "url"
    ]
    cols = [c for c in cols_order if c in df.columns] + [c for c in df.columns if c not in cols_order]
    df = df[cols]

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df.to_csv(output_csv, index=False)
    print(f"✅ Saved cleaned SoFIFA clubs to: {output_csv} (rows={len(df)})")


def parse_args():
    ap = argparse.ArgumentParser(description="Clean SoFIFA club stats into a standardized CSV")
    ap.add_argument(
        "--in",
        dest="input_csv",
        default=os.path.join("Scrapping", "Data", "soFIFA", "Clubs", "club_stats_raw.csv"),
        help="Input raw SoFIFA club stats CSV",
    )
    ap.add_argument(
        "--out",
        dest="output_csv",
        default=os.path.join("Scrapping", "Data", "soFIFA", "Clubs", "club_stats_cleaned.csv"),
        help="Output cleaned CSV path",
    )
    return ap.parse_args()


def main():
    args = parse_args()
    clean_sofifa_clubs(args.input_csv, args.output_csv)


if __name__ == "__main__":
    main()
