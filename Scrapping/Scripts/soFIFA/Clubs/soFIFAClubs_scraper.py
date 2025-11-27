import csv
import os
import re
import asyncio
import argparse
from playwright.async_api import async_playwright


class ClubScraper:
    """Extracts info for one club page"""

    @staticmethod
    async def scrape_club_data(page, url):
        """Extract all club details from one SoFIFA team page"""
        await page.goto(url, wait_until="domcontentloaded", timeout=25000)
        await page.wait_for_timeout(2000)

        data = {"url": url}

        # Run inside browser context for faster DOM access
        club_info = await page.evaluate(
            """
() => {
  const d = {};
  const clean = (t) => (t ? t.replace(/\\s+/g, " ").trim() : "");

  // --- Club ID from URL ---
  const idMatch = window.location.pathname.match(/\\/team\\/(\\d+)\\//);
  d.club_id = idMatch ? idMatch[1] : "";

  // --- Club name ---
  const name = document.querySelector("div.profile h1");
  d.name = clean(name ? name.textContent : "");

  // --- Club logo ---
  const logo = document.querySelector("img.crest");
  d.club_logo = logo ? (logo.dataset.src || logo.src) : "";

  // --- League + League ID ---
  const league = document.querySelector("div.profile p a[href*='/league/']");
  d.league = clean(league ? league.textContent : "");
  const leagueMatch = league ? league.href.match(/\\/league\\/(\\d+)/) : null;
  d.league_id = leagueMatch ? leagueMatch[1] : "";

  // --- Country + Flag ---
  const countryLink = document.querySelector("div.profile p a[title]");
  d.country = clean(countryLink ? countryLink.getAttribute("title") : "");
  const flag = document.querySelector("div.profile p img.flag");
  d.country_flag = flag ? (flag.dataset.src || flag.src) : "";

  // --- Overall / Attack / Midfield / Defence ---
  d.rating = "";
  d.attack_rating = "";
  d.midfield_rating = "";
  d.defense_rating = "";
  const grid = document.querySelectorAll("div.grid div.col");
  grid.forEach(col => {
    const sub = clean(col.querySelector(".sub")?.textContent || "");
    const val = clean(col.querySelector("em")?.textContent || "");
    if (/overall/i.test(sub)) d.rating = val;
    if (/attack/i.test(sub)) d.attack_rating = val;
    if (/midfield/i.test(sub)) d.midfield_rating = val;
    if (/defence|defense/i.test(sub)) d.defense_rating = val;
  });

  // --- Stadium ---
  const stadium = Array.from(document.querySelectorAll("div.col-2 li label"))
    .find(l => /Home stadium/i.test(l.textContent));
  if (stadium) {
    const val = stadium.nextSibling?.textContent || "";
    d.stadium = clean(val);
  } else {
    d.stadium = "";
  }

// --- Manager (from nav-tabs link) ---
const coachLink = document.querySelector('nav.nav-tabs a[href*="/coach/"]');
if (coachLink) {
  const href = coachLink.getAttribute("href");
  const coachMatch = href.match(/\\/coach\\/(\\d+)\\/([\\w-]+)/);
  d.manager = coachMatch
    ? coachMatch[2].replace(/-/g, " ").replace(/\b\\w/g, c => c.toUpperCase())
    : "";
  d.manager_id = coachMatch ? coachMatch[1] : "";
  d.manager_url = coachLink.href;
} else {
  d.manager = "";
  d.manager_id = "";
  d.manager_url = "";
}
    
  // --- Club worth ---
  const worthLi = Array.from(document.querySelectorAll("div.col-2 li label"))
    .find(l => /Club worth/i.test(l.textContent));
  d.club_worth = worthLi
    ? clean(worthLi.parentElement.textContent.replace("Club worth", ""))
    : "";

  // --- Average ages ---
  const xiAgeLi = Array.from(document.querySelectorAll("div.col-2 li label"))
    .find(l => /Starting XI average age/i.test(l.textContent));
  const teamAgeLi = Array.from(document.querySelectorAll("div.col-2 li label"))
    .find(l => /Whole team average age/i.test(l.textContent));
  d.starting_xi_avg_age = xiAgeLi
    ? clean(xiAgeLi.parentElement.textContent.replace("Starting XI average age", ""))
    : "";
  d.whole_team_avg_age = teamAgeLi
    ? clean(teamAgeLi.parentElement.textContent.replace("Whole team average age", ""))
    : "";

  // --- Rival club ---
  const rivalLi = Array.from(document.querySelectorAll("div.col-2 li label"))
    .find(l => /Rival team/i.test(l.textContent));
  d.rival_team = rivalLi
    ? clean(rivalLi.parentElement.textContent.replace("Rival team", ""))
    : "";

  // --- Players (from lineup field baskets) ---
  const playerLinks = document.querySelectorAll("div.field-basket ul a[href*='/player/']");
  const players = Array.from(playerLinks).map(a => clean(a.textContent)).filter(Boolean);
  d.players_count = players.length;
  d.top_players = players.slice(0, 5).join(", ");

  return d;
}
"""
        )

        data.update(club_info)
        return data


class SoFIFAClubScraper:
  """Main scraper that loops through all club URLs and saves to CSV"""

  def __init__(self, fresh: bool = False):
    # Locate 'Scrapping' folder robustly, then use its parent as repo root
    current_dir = os.path.dirname(os.path.abspath(__file__))
    scrapping_root = current_dir
    while True:
      if os.path.basename(scrapping_root) == "Scrapping":
        break
      parent = os.path.dirname(scrapping_root)
      if parent == scrapping_root:
        raise RuntimeError("Could not locate 'Scrapping' directory above script path")
      scrapping_root = parent

    project_root = os.path.dirname(scrapping_root)
    data_dir = os.path.join(project_root, "Data", "soFIFA", "Clubs")
    os.makedirs(data_dir, exist_ok=True)

    self.urls_file = os.path.join(data_dir, "club_urls.csv")
    self.output_file = os.path.join(data_dir, "club_stats_raw.csv")
    self.club_urls: list[str] = []
    self.results: list[dict] = []
    self.existing_urls: set[str] = set()
    self.existing_ids: set[str] = set()

    # Load existing progress unless fresh run requested
    if not fresh and os.path.exists(self.output_file):
      self._load_existing_progress()

  @staticmethod
  def _extract_club_id_from_url(url: str) -> str:
    m = re.search(r"/team/(\d+)/", url)
    return m.group(1) if m else ""

  def _load_existing_progress(self) -> None:
    try:
      with open(self.output_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
          u = (row.get("url") or "").strip()
          cid = (row.get("club_id") or "").strip()
          if u:
            self.existing_urls.add(u)
          if cid:
            self.existing_ids.add(cid)
      print(f"[resume] Loaded {len(self.existing_urls)} existing clubs from {os.path.basename(self.output_file)}")
    except Exception as e:
      print(f"[resume] Warning: couldn't read existing clubs ({e})")

  def load_urls(self) -> None:
    if not os.path.exists(self.urls_file):
      raise FileNotFoundError(
        f"club_urls.csv not found at {self.urls_file}. Run soFIFAClubs_url_scraper.py first."
      )
    with open(self.urls_file, "r", encoding="utf-8") as f:
      next(f, None)
      self.club_urls = [line.strip() for line in f if line.strip()]
    print(f"✅ Loaded {len(self.club_urls)} club URLs")

  async def scrape_all_clubs(self, limit: int | None = None) -> None:
    async with async_playwright() as p:
      browser = await p.chromium.launch(
        headless=True,
        args=[
          "--disable-blink-features=AutomationControlled",
          "--disable-dev-shm-usage",
          "--no-sandbox",
          "--disable-setuid-sandbox",
        ],
      )
      context = await browser.new_context(
        user_agent=(
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
          "AppleWebKit/537.36 (KHTML, like Gecko) "
          "Chrome/120.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1920, "height": 1080},
        locale="en-US",
        timezone_id="America/New_York",
      )
      page = await context.new_page()
      await page.route(
        "**/*",
        lambda route: (
          route.abort()
          if route.request.resource_type in ["image", "font", "stylesheet", "media"]
          else route.continue_()
        ),
      )

      # Filter already-scraped clubs by URL or club_id parsed from URL
      filtered: list[str] = []
      skipped = 0
      for u in self.club_urls:
        cid = self._extract_club_id_from_url(u)
        if (u in self.existing_urls) or (cid and cid in self.existing_ids):
          skipped += 1
          continue
        filtered.append(u)

      target_all = filtered
      target = target_all[:limit] if limit else target_all
      if skipped:
        print(f"[resume] Skipping {skipped} already-scraped clubs; remaining {len(target_all)}")
      total = len(target)
      for idx, url in enumerate(target, 1):
        print(f"[{idx}/{total}] Scraping {url}")
        try:
          data = await ClubScraper.scrape_club_data(page, url)
          self.results.append(data)
          self.save_to_csv(data)
          print(
            f"  ✓ {data.get('name', 'Unknown Club')} ({data.get('league', 'Unknown League')})"
          )
        except Exception as e:
          print(f"  ✗ Error scraping {url}: {e}")
      await browser.close()

    print(f"\n✅ Finished scraping {len(self.results)} clubs")
    print(f"💾 Raw results saved to: {self.output_file}")

  def save_to_csv(self, data: dict) -> None:
    file_exists = os.path.isfile(self.output_file)
    fieldnames = [
      "club_id","name","league","league_id","country","rating","attack_rating",
      "midfield_rating","defense_rating","stadium","manager","manager_id","manager_url",
      "club_worth","starting_xi_avg_age","whole_team_avg_age","rival_team","players_count",
      "top_players","club_logo","country_flag","url"
    ]
    with open(self.output_file, "a", newline="", encoding="utf-8") as f:
      writer = csv.DictWriter(f, fieldnames=fieldnames)
      if not file_exists:
        writer.writeheader()
      writer.writerow(data)


def parse_args():
    ap = argparse.ArgumentParser(description="SoFIFA Clubs stats scraper")
    ap.add_argument("--limit", type=int, default=None, help="Optional number of clubs to scrape")
    ap.add_argument("--fresh", action="store_true", help="Start with a fresh club_stats.csv (overwrite existing)")
    return ap.parse_args()


async def main():
    args = parse_args()
    scraper = SoFIFAClubScraper(fresh=args.fresh)
    print("=" * 60)
    print("SoFIFA Club Scraper")
    print("=" * 60)

    scraper.load_urls()
    if args.fresh and os.path.exists(scraper.output_file):
        os.remove(scraper.output_file)
        print("🧼 Fresh mode: existing club_stats_raw.csv removed.")
    await scraper.scrape_all_clubs(limit=args.limit)


if __name__ == "__main__":
    asyncio.run(main())
