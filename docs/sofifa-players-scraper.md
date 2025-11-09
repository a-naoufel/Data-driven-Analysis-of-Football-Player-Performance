# SoFIFA Players Scraper & Cleaner

This guide explains how to scrape player URLs and detailed player stats from SoFIFA, store raw CSVs in the repo's Scrapping/Data tree, and produce a cleaned dataset under the same folder.

## Outputs

- Raw URL list: `Scrapping/Data/soFIFA/Players/player_urls.csv`
- Raw player stats: `Scrapping/Data/soFIFA/Players/player_stats_raw.csv`
- Cleaned player stats: `Scrapping/Data/soFIFA/Players/player_stats_cleaned.csv`

## 1) Scrape player profile URLs

Script: `Scrapping/Scripts/soFIFA/Players/scrape_player_urls.py`

- Saves incrementally after each page
- Retries on Cloudflare pages, blocks heavy resources
- Exact URL limit support (optional)

Examples (PowerShell):

- Scrape all URLs (may take a while):
  - python Scrapping/Scripts/soFIFA/Players/scrape_player_urls.py

- Scrape the first 300 URLs exactly:
  - python Scrapping/Scripts/soFIFA/Players/scrape_player_urls.py --limit 300

## 2) Scrape detailed player stats

Script: `Scrapping/Scripts/soFIFA/Players/sofifa_scraper.py`

Key flags:
- `--max-players N` limit the number of players to scrape from the URL CSV
- `--player-urls-file` (default: `player_urls.csv`) file name inside `Scrapping/Data/soFIFA/Players/`
- `--output-file` (default: `player_stats_raw.csv`) output file name inside `Scrapping/Data/soFIFA/Players/`
- `--fresh-stats` delete any existing output file before scraping

Examples (PowerShell):

- Scrape the first 100 players:
  - python Scrapping/Scripts/soFIFA/Players/sofifa_scraper.py --max-players 100

- Fresh run writing to a custom file:
  - python Scrapping/Scripts/soFIFA/Players/sofifa_scraper.py --fresh-stats --output-file player_stats_raw.sample.csv --max-players 50

Notes:
- The scraper writes each player row immediately (durable, restart-friendly).
- It uses a realistic user-agent and blocks images/styles to speed up loads.
 - Resume is automatic: if `Scrapping/Data/soFIFA/Players/player_stats_raw.csv` exists, the scraper reads it,
   skips already-scraped players (by `url`/`player_id`), and appends new rows. Use `--fresh-stats` to start over.

## 3) Clean the raw player stats

Script: `Processing/soFIFA/Players/clean_sofifa_players.py`

What it does:
- Normalizes whitespace in text fields
- Converts numeric fields (ratings, money, attributes) to numbers
- Deduplicates `(player_id, version)`
- Preserves all columns while ordering the most important first

Examples (PowerShell):

- Clean the default raw file:
  - python Processing/soFIFA/Players/clean_sofifa_players.py

- Clean a custom input into a custom output:
  - python Processing/soFIFA/Players/clean_sofifa_players.py --in Scrapping/Data/soFIFA/Players/player_stats_raw.sample.csv --out Scrapping/Data/soFIFA/Players/player_stats_cleaned.sample.csv

## Schema highlights

Common columns present for most players:
- Identifiers: `player_id`, `version`, `url`
- Names: `name`, `full_name`
- Bio: `dob`, `height_cm`, `weight_kg`, `preferred_foot`, `body_type`, `real_face`
- Overview: `positions`, `overall_rating`, `potential`, `value`, `wage`, `release_clause`
- Club: `club_id`, `club_name`, `club_league_id`, `club_league_name`, `club_rating`, `club_position`, `club_kit_number`, `club_joined`, `club_contract_valid_until`
- National team: `country_id`, `country_name`, `country_league_id`, `country_league_name`, `country_rating`, `country_position`, `country_kit_number`
- Attributes: sections prefixed by `attacking_`, `skill_`, `movement_`, `power_`, `mentality_`, `defending_`, `goalkeeping_`
- Extras: `play_styles`, `image`

If you need a minimal subset for analysis, filter the cleaned CSV to your preferred columns.
