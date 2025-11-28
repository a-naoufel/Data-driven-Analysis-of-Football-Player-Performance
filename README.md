# Data-driven Analysis of Football Player Performance

- Scrapes player and club data from SoFIFA and Transfermarkt.
- Cleans and filters datasets, then merges players via exact + fuzzy matching.
- Outputs CSVs under `Data/` (raw, cleaned, filtered, merged, unresolved).

Quick run examples:
- SoFIFA players: `python Scrapping/Scripts/soFIFA/Players/scrape_player_urls.py` then `python Scrapping/Scripts/soFIFA/Players/sofifa_scraper.py`
- Transfermarkt players: `python Scrapping/Scripts/Transfermarket/transfermarkt_players.py`
- Merge players: `python Processing/soFIFA/Players/merge_players.py`