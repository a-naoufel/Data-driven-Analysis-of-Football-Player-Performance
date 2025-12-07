# Transfermarkt Clubs Scraper

Scrapes Transfermarkt league pages to collect clubs that match your SoFIFA set.

## Outputs
- Raw leagues scan: `Data/Transfermarkt/Clubs/transfermarkt_dynamic_leagues.csv`
- SoFIFA-only clubs: `Data/Transfermarkt/Clubs/transfermarkt_clubs_sofifa_only.csv`

## Input
- SoFIFA clubs cleaned: `Data/soFIFA/Clubs/club_stats_cleaned.csv`

## Run (PowerShell)
- python Scrapping/Scripts/Transfermarket/transfermarkt_clubs.py

Notes:
- Uses Playwright in headless mode with a desktop user-agent.
- Contains a mapping of league names → Transfermarkt URLs, with a few fallbacks.
- Filters scraped clubs to the SoFIFA set by normalized league+club keys.