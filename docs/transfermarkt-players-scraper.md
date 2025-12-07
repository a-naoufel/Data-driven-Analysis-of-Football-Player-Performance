# Transfermarkt Players Scraper

Collects squad players for clubs found on Transfermarkt (filtered to your SoFIFA set), then writes raw and filtered CSVs.

## Outputs
- Raw players: `Data/Transfermarkt/Players/players_from_365.csv`
- Filtered players: `Data/Transfermarkt/Players/players_from_365.filtered.csv`

## Inputs
- Clubs list: `Data/Transfermarkt/Clubs/transfermarkt_clubs_sofifa_only.csv` (from the clubs scraper)

## Run (PowerShell)
- python Scrapping/Scripts/Transfermarket/transfermarkt_players.py
- Optional: `--limit N` to process first N clubs

## Dedupe-only helper
- To remove duplicates while keeping all columns (keep first occurrence):
  - python Scrapping/Scripts/Transfermarket/remove_duplicates.py

Notes:
- Uses Playwright (sync) in headless mode; accepts cookie prompts.
- Extracts each row’s market value and a numeric `market_value_eur` where possible.
- Saves after scraping all clubs; use the helper to re-filter an existing copy if needed.
