# SoFIFA Clubs Scraper

This document explains how to use the SoFIFA clubs scrapers that collect club URLs and detailed club information.

## Overview

There are two stages:
- URL scraper (`Scrapping/Scripts/soFIFA/Clubs/soFIFAClubs_url_scraper.py`): crawls the SoFIFA club list and appends each discovered club URL to a CSV as it goes.
- Club stats scraper (`Scrapping/Scripts/soFIFA/Clubs/soFIFAClubs_scraper.py`): reads the URLs CSV and visits each club page to extract details, appending each club's data to a CSV incrementally.

You can also run both in sequence using the orchestrator:
- Orchestrator (`Scrapping/Scripts/soFIFA/Clubs/soFIFAClubs.py`): runs the URL scraper first, then the stats scraper.

## Output locations

- Club URLs: `Scrapping/Data/soFIFA/Clubs/club_urls.csv`
- Club stats: `Scrapping/Data/soFIFA/Clubs/club_stats.csv`

These files are written incrementally, so progress is saved even if the run stops early.

## Prerequisites

- Python 3.10+
- Dependencies from `requirements.txt` (includes Playwright)
- One-time Playwright browser install

```pwsh
pip install -r requirements.txt
python -m playwright install
```

## Usage

Run both stages via the orchestrator:

```pwsh
# Fresh full scrape with limits
python Scrapping/Scripts/soFIFA/Clubs/soFIFAClubs.py --fresh-urls --fresh-stats --url-limit 120 --club-limit 20

# Incremental (continues existing files)
python Scrapping/Scripts/soFIFA/Clubs/soFIFAClubs.py --url-limit 120 --club-limit 20

# Full run (no limits, incremental)
python Scrapping/Scripts/soFIFA/Clubs/soFIFAClubs.py
```

Run URL scraper alone:

```pwsh
# Collect up to 180 URLs (incremental)
python Scrapping/Scripts/soFIFA/Clubs/soFIFAClubs_url_scraper.py --limit 180

# Fresh run for exactly 60 URLs
python Scrapping/Scripts/soFIFA/Clubs/soFIFAClubs_url_scraper.py --fresh --limit 60
```

Run club stats scraper alone:

```pwsh
# Scrape stats for first 15 URLs (incremental append)
python Scrapping/Scripts/soFIFA/Clubs/soFIFAClubs_scraper.py --limit 15

# Fresh stats run for 25 clubs
python Scrapping/Scripts/soFIFA/Clubs/soFIFAClubs_scraper.py --fresh --limit 25
```

## Arguments

URL scraper:
- `--limit` (int, optional): max number of club URLs to collect.
- `--base-url` (str, optional): base SoFIFA clubs list URL (defaults to rating-desc order).
- `--fresh` (flag): overwrite any existing `club_urls.csv` and start clean.

Club stats scraper:
- `--limit` (int, optional): max number of clubs to process.
- `--fresh` (flag): remove existing `club_stats.csv` before writing.

Orchestrator:
- `--url-limit` (int, optional): passed to URL scraper.
- `--club-limit` (int, optional): passed to stats scraper.
- `--fresh-urls` (flag): reset `club_urls.csv` before URL scraping.
- `--fresh-stats` (flag): remove `club_stats.csv` before stats scraping.

## Data schema

`club_urls.csv`
- `club_url` (str): absolute link to a SoFIFA club page.

`club_stats.csv` (columns written in this order)
- `club_id` (str)
- `name` (str)
- `league` (str)
- `league_id` (str)
- `country` (str)
- `rating` (str)
- `attack_rating` (str)
- `midfield_rating` (str)
- `defense_rating` (str)
- `stadium` (str)
- `manager` (str)
- `manager_id` (str)
- `manager_url` (str)
- `club_worth` (str)
- `starting_xi_avg_age` (str)
- `whole_team_avg_age` (str)
- `rival_team` (str)
- `players_count` (int)
- `top_players` (str)
- `club_logo` (str)
- `country_flag` (str)
- `url` (str) – source club URL

## Tips

- The scrapers are resilient and block heavy resources for speed.
- You can safely re-run the URL scraper; it seeds from existing `club_urls.csv` and only appends new URLs.
- If you need to resume club stats scraping, you can implement a simple skip logic by reading already-scraped `club_id`s and skipping those URLs.
