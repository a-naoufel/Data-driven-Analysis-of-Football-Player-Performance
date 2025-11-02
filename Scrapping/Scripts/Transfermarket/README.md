# Transfermarkt pipeline (dcaribou/transfermarkt-scraper)

This folder contains an all-purpose pipeline `tfmkt_pipeline.py` that orchestrates the maintained Transfermarkt Scrapy project to collect competitions, clubs, and players, while aligning clubs to your cleaned SoFIFA leagues.

## Prerequisites

- Docker Desktop installed and running
- Internet access
- A valid browser User-Agent string
- The SoFIFA cleaned CSV at `Scrapping/Data/Clubs/sofifa_clubs_clean.csv`

## Quick start (Windows PowerShell)

- Set your user agent for this session:

```powershell
$env:TFMKT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
```

- Check environment and paths:

```powershell
python .\Scrapping\Scripts\Transfermarket\tfmkt_pipeline.py --check
```

- Run a full scrape (default leagues inferred from SoFIFA):

```powershell
python .\Scrapping\Scripts\Transfermarket\tfmkt_pipeline.py --season current
```

- Limit to certain leagues (names as they appear in SoFIFA cleaned CSV):

```powershell
python .\Scrapping\Scripts\Transfermarket\tfmkt_pipeline.py --leagues "Premier League, La Liga, Bundesliga"
```

## Outputs

Generated under `Scrapping/Data/Transfermarkt/`:

- `confederations.jsonl`
- `competitions.jsonl`, `competitions.filtered.jsonl`
- `clubs.jsonl`, `clubs.filtered.jsonl`, `clubs.filtered.csv`
- `players.jsonl`, `players.filtered.jsonl`, `players.filtered.csv`

Clubs are filtered to only those present in your SoFIFA cleaned CSV (by normalized league+club). Players are collected for the filtered clubs.

## Notes

- The pipeline runs the official Docker image `dcaribou/transfermarkt-scraper:main` and mounts the repository root at `/app`. No local install of Scrapy or the project is necessary.
- You can pass `--user-agent` instead of setting the environment variable.
- Competition name synonyms are handled for common cases (e.g., `Ligue 1 Uber Eats` -> `Ligue 1`, `Primeira Liga` -> `Liga Portugal`, `MLS` -> `Major League Soccer`, etc.).