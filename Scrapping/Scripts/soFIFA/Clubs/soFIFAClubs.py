import os
import asyncio
import argparse
from soFIFAClubs_url_scraper import ClubURLScraper
from soFIFAClubs_scraper import SoFIFAClubScraper


def parse_args():
    ap = argparse.ArgumentParser(description="Full SoFIFA clubs scraper (URLs + stats)")
    ap.add_argument("--url-limit", type=int, default=None, help="Limit number of club URLs to collect")
    ap.add_argument("--club-limit", type=int, default=None, help="Limit number of clubs to scrape stats for")
    ap.add_argument("--fresh-urls", action="store_true", help="Start with a fresh club_urls.csv (overwrite existing)")
    ap.add_argument("--fresh-stats", action="store_true", help="Start with a fresh club_stats.csv (overwrite existing)")
    return ap.parse_args()


async def main():
    args = parse_args()
    print("=" * 60)
    print("⚽ SoFIFA Clubs Full Scraper")
    print("=" * 60)

    # Step 1: Scrape club URLs
    print("\nSTEP 1️⃣: Scraping all club URLs...")
    url_scraper = ClubURLScraper(limit=args.url_limit)
    if args.fresh_urls:
        with open(url_scraper.output_file, "w", newline="", encoding="utf-8") as f:
            csv_writer = __import__("csv").writer(f)
            csv_writer.writerow(["club_url"])
        # clear internal state if any
        url_scraper.all_club_urls = []
        print("🧼 Fresh mode: reset club_urls.csv")
    await url_scraper.scrape_all_club_urls()
    print("✅ Club URLs scraping done.\n")

    # Step 2: Scrape club stats
    print("STEP 2️⃣: Scraping club statistics...")
    club_scraper = SoFIFAClubScraper()
    club_scraper.load_urls()
    if args.fresh_stats and os.path.exists(club_scraper.output_file):
        os.remove(club_scraper.output_file)
        print("🧼 Fresh mode: removed existing club_stats.csv")
    await club_scraper.scrape_all_clubs(limit=args.club_limit)
    print("✅ Club stats scraping done.\n")

    print("=" * 60)
    print("🎉 ALL DONE — CSV files are ready!")
    print("=" * 60)
    print("➡ club_urls.csv and club_stats.csv saved in: Data/soFIFA/Clubs/")


if __name__ == "__main__":
    asyncio.run(main())
