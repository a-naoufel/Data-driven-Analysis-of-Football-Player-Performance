"""
All-purpose Transfermarkt pipeline using dcaribou/transfermarkt-scraper (Docker).

Pipeline:
  confederations -> competitions (filter to desired leagues) -> clubs (filter to SoFIFA clubs) -> players

Outputs (under Scrapping/Data/Transfermarkt/):
  competitions.jsonl, competitions.filtered.jsonl
  clubs.jsonl, clubs.filtered.jsonl, clubs.filtered.csv
  players.jsonl, players.filtered.jsonl, players.filtered.csv

Requirements:
  - Docker available and image `dcaribou/transfermarkt-scraper:main`
  - USER AGENT set via: env TFMKT_USER_AGENT or --user-agent arg
  - SoFIFA cleaned clubs CSV at Scrapping/Data/Clubs/sofifa_clubs_clean.csv

You can run env checks first:
  python tfmkt_pipeline.py --check

Run a full scrape (default leagues inferred from SoFIFA):
  python tfmkt_pipeline.py --season current

Limit to certain leagues (comma-separated, match SoFIFA names):
  python tfmkt_pipeline.py --leagues "Premier League, La Liga, Bundesliga"
"""

from __future__ import annotations

import argparse
import os
import sys
import json
import subprocess
import shutil
from typing import Iterable, Dict, Any, List, Set

import pandas as pd
import unicodedata


HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "../..", ".."))
DATA_DIR = os.path.join(ROOT, "Scrapping", "Data")
CLUBS_DATA_DIR = os.path.join(DATA_DIR, "Clubs")
OUT_DIR = os.path.join(DATA_DIR, "Transfermarkt")
os.makedirs(OUT_DIR, exist_ok=True)

SOFIFA_CLUBS = os.path.join(CLUBS_DATA_DIR, "sofifa_clubs_clean.csv")


def norm(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace("\u2013", "-").replace("\u2014", "-")
    return " ".join(s.lower().split())


LEAGUE_SYNONYMS: Dict[str, Set[str]] = {
    # SoFIFA -> acceptable Transfermarkt competition names (normalized)
    "ligue 1 uber eats": {"ligue 1"},
    "ligue 2 bkt": {"ligue 2"},
    "primeira liga": {"liga portugal", "liga portugal bwin", "primeira liga"},
    "la liga": {"laliga", "primera division", "primera división"},
    "la liga 2": {"laliga2", "segunda division", "segunda división", "laliga hypermotion"},
    "scottish premiership": {"scottish premiership", "premiership"},
    "belgian pro league": {"jupiler pro league", "pro league"},
    "greek super league": {"super league", "super league 1", "super league greece"},
    "saudi pro league": {"saudi professional league", "saudi pro league"},
    "uae pro league": {"uae pro league", "arabian gulf league"},
    "major league soccer": {"major league soccer", "mls"},
    "a-league men": {"a-league", "a-league men"},
}


def get_user_agent(cli: str | None) -> str:
    ua = cli or os.getenv("TFMKT_USER_AGENT") or os.getenv("USER_AGENT")
    if not ua:
        raise SystemExit(
            "A user agent is required. Provide via --user-agent or TFMKT_USER_AGENT env var."
        )
    return ua


def check_docker() -> bool:
    return shutil.which("docker") is not None


def run_docker_scrapy(commands: List[str], user_agent: str, workdir: str, outfile: str) -> None:
    """Run scrapy crawl inside the official docker image and write JSON lines to outfile."""
    img = "dcaribou/transfermarkt-scraper:main"
    # Ensure image present (pull if needed)
    pull = subprocess.run(["docker", "pull", img], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if pull.returncode != 0:
        print(pull.stdout)
        raise SystemExit("Failed to pull Docker image 'dcaribou/transfermarkt-scraper:main'")

    cmd = [
        "docker", "run", "--rm",
        "-v", f"{workdir}:/app",
        img,
        "bash", "-lc",
        # build a single scrapy command with user agent set
        f"scrapy {' '.join(commands)} -s USER_AGENT=\"{user_agent}\""
    ]

    # Call Docker and capture stdout
    print(f"Running: {' '.join(cmd)}")
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if proc.returncode != 0:
        print(proc.stdout)
        raise SystemExit("Docker scrapy command failed")

    with open(outfile, "w", encoding="utf-8") as f:
        f.write(proc.stdout)


def read_jsonl(path: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except json.JSONDecodeError:
                # ignore noisy lines
                continue
    return items


def write_jsonl(path: str, items: Iterable[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")


def infer_league_allowlist(sofifa_csv: str, cli_leagues: List[str] | None) -> Set[str]:
    if cli_leagues:
        leagues = [norm(x) for x in cli_leagues]
        return set(leagues)
    df = pd.read_csv(sofifa_csv)
    if "league_name" not in df.columns:
        raise SystemExit("SoFIFA CSV must include 'league_name' column")
    return set(df["league_name"].dropna().map(norm).unique().tolist())


def filter_competitions(all_comp: List[Dict[str, Any]], allowed_leagues: Set[str]) -> List[Dict[str, Any]]:
    filtered = []
    for c in all_comp:
        name = c.get("name") or c.get("competition_name") or c.get("title") or ""
        n = norm(name)
        if n in allowed_leagues:
            filtered.append(c)
            continue
        # syns
        for base, syns in LEAGUE_SYNONYMS.items():
            if base in allowed_leagues and n in syns:
                filtered.append(c)
                break
    return filtered


def filter_clubs(all_clubs: List[Dict[str, Any]], sofifa_csv: str) -> List[Dict[str, Any]]:
    df = pd.read_csv(sofifa_csv)
    # normalize
    df["_key"] = df["league_name"].map(norm) + "||" + df["club"].map(norm)
    keys = set(df["_key"].tolist())

    kept: List[Dict[str, Any]] = []
    for club in all_clubs:
        comp = club.get("competition_name") or club.get("competition") or club.get("league") or ""
        name = club.get("club_name") or club.get("name") or ""
        if not name:
            continue
        k = norm(comp) + "||" + norm(name)
        # direct
        if k in keys:
            kept.append(club)
            continue
        # attempt synonyms on competition
        matched = False
        for base, syns in LEAGUE_SYNONYMS.items():
            if norm(comp) in syns:
                k2 = base + "||" + norm(name)
                if k2 in keys:
                    kept.append(club)
                    matched = True
                    break
        if matched:
            continue
    return kept


def to_csv(jsonl_path: str, csv_path: str, columns: List[str] | None = None) -> None:
    if not os.path.exists(jsonl_path):
        return
    rows = read_jsonl(jsonl_path)
    if not rows:
        return
    df = pd.DataFrame(rows)
    if columns:
        # only keep columns that exist
        cols = [c for c in columns if c in df.columns]
        if cols:
            df = df[cols]
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", default="current", help="Season to crawl (default: current)")
    ap.add_argument("--leagues", default=None, help="Comma-separated list of SoFIFA league names to include")
    ap.add_argument("--user-agent", dest="ua", default=None, help="User-Agent for Transfermarkt")
    ap.add_argument("--check", action="store_true", help="Run environment checks and exit")
    args = ap.parse_args(argv)

    # Checks
    if not os.path.exists(SOFIFA_CLUBS):
        print(f"Missing SoFIFA CSV: {SOFIFA_CLUBS}")
        return 2

    ua = None
    try:
        ua = get_user_agent(args.ua)
    except SystemExit as e:
        if not args.check:
            raise
        print(str(e))

    docker_ok = check_docker()
    if args.check:
        print(f"Docker available: {docker_ok}")
        print(f"User-Agent set: {bool(ua)}")
        print(f"Out dir: {OUT_DIR}")
        return 0

    if not docker_ok:
        print("Docker is required for this pipeline. Install Docker Desktop and retry.")
        return 3

    leagues = None
    if args.leagues:
        leagues = [x.strip() for x in args.leagues.split(",") if x.strip()]
    allowed = infer_league_allowlist(SOFIFA_CLUBS, leagues)

    # 1) confederations
    conf_path = os.path.join(OUT_DIR, "confederations.jsonl")
    run_docker_scrapy(["crawl", "confederations"], ua, ROOT, conf_path)

    # 2) competitions
    comps_path = os.path.join(OUT_DIR, "competitions.jsonl")
    run_docker_scrapy(["crawl", "competitions", "-a", f"parents=Scrapping/Data/Transfermarkt/confederations.jsonl"], ua, ROOT, comps_path)

    comps = read_jsonl(comps_path)
    comps_f = filter_competitions(comps, allowed)
    comps_f_path = os.path.join(OUT_DIR, "competitions.filtered.jsonl")
    write_jsonl(comps_f_path, comps_f)
    print(f"Filtered competitions: {len(comps_f)}")

    # 3) clubs from filtered competitions
    clubs_path = os.path.join(OUT_DIR, "clubs.jsonl")
    run_docker_scrapy(["crawl", "clubs", "-a", f"parents=Scrapping/Data/Transfermarkt/competitions.filtered.jsonl"], ua, ROOT, clubs_path)

    clubs = read_jsonl(clubs_path)
    clubs_f = filter_clubs(clubs, SOFIFA_CLUBS)
    clubs_f_path = os.path.join(OUT_DIR, "clubs.filtered.jsonl")
    write_jsonl(clubs_f_path, clubs_f)
    to_csv(clubs_f_path, os.path.join(OUT_DIR, "clubs.filtered.csv"), columns=[
        "competition_name", "club_id", "club_name", "country", "squad_size", "total_market_value"
    ])
    print(f"Filtered clubs: {len(clubs_f)}")

    # 4) players for filtered clubs
    players_path = os.path.join(OUT_DIR, "players.jsonl")
    run_docker_scrapy(["crawl", "players", "-a", f"parents=Scrapping/Data/Transfermarkt/clubs.filtered.jsonl"], ua, ROOT, players_path)

    players = read_jsonl(players_path)
    # keep as-is; optionally filter later against SoFIFA players when integrating attributes
    players_f_path = os.path.join(OUT_DIR, "players.filtered.jsonl")
    write_jsonl(players_f_path, players)
    to_csv(players_f_path, os.path.join(OUT_DIR, "players.filtered.csv"))
    print(f"Players collected: {len(players)}")

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
