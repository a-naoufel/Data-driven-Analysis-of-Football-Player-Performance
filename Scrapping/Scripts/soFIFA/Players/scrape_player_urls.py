"""
SoFIFA Player URL Scraper
Scrapes all player URLs from sofifa.com paginated list
"""
import csv
import asyncio
from playwright.async_api import async_playwright
import os
import argparse


class PlayerURLScraper:
    def __init__(self, base_url="https://sofifa.com/players?col=oa&sort=desc", limit: int | None = None, output_filename: str = "player_urls.csv"):
        self.base_url = base_url
        self.all_player_urls = []
        self.offset = 0
        self.page_size = 60
        self.limit = limit
        # Precompute output directory for consistency
        self.output_filename = output_filename

    async def scrape_all_player_urls(self):
        """Scrape all player URLs from paginated list"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox'
                ]
            )
            
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                timezone_id='America/New_York',
                extra_http_headers={
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none'
                }
            )
            
            page = await context.new_page()
            
            # Block images, stylesheets, fonts to optimize loading
            await page.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "stylesheet", "font", "media"] else route.continue_())
            
            page_num = 1
            has_next = True
            
            while has_next:
                url = f"{self.base_url}&offset={self.offset}" if self.offset > 0 else self.base_url
                
                retries = 0
                max_retries = 3
                success = False
                
                while retries < max_retries and not success:
                    try:
                        if retries > 0:
                            print(f"  Retry {retries}/{max_retries} after 10s pause...")
                            await asyncio.sleep(10)
                        
                        print(f"\n[Page {page_num}] Scraping: {url}")
                        
                        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                        await page.wait_for_timeout(2000)
                        
                        # Check for Cloudflare challenge
                        page_content = await page.content()
                        if 'Checking your browser' in page_content or 'Just a moment' in page_content or 'cf-browser-verification' in page_content:
                            print("  ⚠ Cloudflare challenge detected")
                            retries += 1
                            continue
                        
                        # Extract player URLs from current page
                        page_data = await page.evaluate("""
                            () => {
                                const urls = [];
                                const links = document.querySelectorAll('a[href*="/player/"]');
                                
                                links.forEach(link => {
                                    const href = link.href;
                                    // Only get unique player profile URLs (not random links)
                                    if (href && href.includes('/player/') && !href.includes('random') && !urls.includes(href)) {
                                        urls.push(href);
                                    }
                                });
                                
                                // Check if "Next" button exists
                                const nextButton = [...document.querySelectorAll('a.button')].find(a => a.textContent.includes('Next'));
                                const hasNext = Boolean(nextButton);
                                
                                return { urls, hasNext };
                            }
                        """)
                        
                        player_urls = page_data['urls']
                        has_next = page_data['hasNext']
                        
                        print(f"  ✓ Extracted {len(player_urls)} player URLs")
                        print(f"  Next button exists: {has_next}")
                        
                        # Add to collection (dedupe while preserving order)
                        for u in player_urls:
                            if u not in self.all_player_urls:
                                self.all_player_urls.append(u)

                        # Enforce exact limit mid-pagination if requested
                        if self.limit is not None and len(self.all_player_urls) >= self.limit:
                            self.all_player_urls = self.all_player_urls[: self.limit]
                            # Save and stop
                            self.save_urls_to_csv()
                            success = True
                            has_next = False
                            break

                        # Save after each page
                        self.save_urls_to_csv()
                        
                        success = True
                        
                        # Move to next page
                        if has_next:
                            self.offset += self.page_size
                            page_num += 1
                        
                    except Exception as e:
                        print(f"  ✗ Error: {str(e)}")
                        retries += 1
                        if retries >= max_retries:
                            print(f"  ✗ Failed after {max_retries} retries")
                            has_next = False  # Stop pagination on failure
            
            await browser.close()
            
        return self.all_player_urls

    def save_urls_to_csv(self, filename: str | None = None):
        """Save all player URLs to CSV file in Data/soFIFA/Players/player_urls.csv"""

        # Get the absolute path of the current script (e.g. Scrapping/Scripts/soFIFA/Players/)
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # Go four levels up to reach the repository root (Players -> soFIFA -> Scripts -> Scrapping -> repo)
        repo_root = os.path.abspath(os.path.join(current_dir, "..", "..", "..", ".."))

        # Build the target data directory path (Data/soFIFA/Players)
        data_dir = os.path.join(
            repo_root,
            "Data",
            "soFIFA",
            "Players",
        )

        # Ensure the directory exists
        os.makedirs(data_dir, exist_ok=True)

        out_name = filename or self.output_filename
        filepath = os.path.join(data_dir, out_name)

        # Remove duplicates while preserving order
        unique_urls = []
        seen = set()
        for url in self.all_player_urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['player_url'])
            for url in unique_urls:
                writer.writerow([url])

        print(f"  💾 Saved {len(unique_urls)} unique URLs to {out_name}")


def parse_args():
    ap = argparse.ArgumentParser(description="Scrape SoFIFA player profile URLs")
    ap.add_argument("--limit", type=int, default=None, help="Stop after collecting exactly this many URLs")
    ap.add_argument("--output-file", default="player_urls.csv", help="Output CSV filename (saved under Data/soFIFA/Players)")
    return ap.parse_args()


async def main():
    """Main function to run the URL scraper"""
    args = parse_args()
    scraper = PlayerURLScraper(limit=args.limit, output_filename=args.output_file)
    
    print("="*60)
    print("SoFIFA Player URL Scraper")
    print("="*60)
    print("\nThis will scrape player URLs from sofifa.com")
    print("The process may take several minutes...")
    print("\nFeatures:")
    print("  - Headless mode (no browser window)")
    print("  - Resource blocking for faster loading")
    print("  - Cloudflare retry with 10s backoff (3 retries)")
    print("  - Saves after each page")
    if args.limit is not None:
        print(f"  - Exact limit enforced: {args.limit} URLs")
    print("="*60)
    
    await scraper.scrape_all_player_urls()
    
    # Final save
    scraper.save_urls_to_csv()
    
    # Print summary
    print("\n" + "="*60)
    print("SCRAPING COMPLETED!")
    print("="*60)
    print(f"Total unique player URLs: {len(set(scraper.all_player_urls))}")
    print(f"Total pages scraped: {(scraper.offset // scraper.page_size) + 1}")
    print("\nFile created:")
    print("  -", args.output_file)
    print("\nYou can now run sofifa_scraper.py to scrape player stats!")


if __name__ == "__main__":
    asyncio.run(main())
