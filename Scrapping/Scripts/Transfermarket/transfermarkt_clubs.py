from playwright.sync_api import sync_playwright
import pandas as pd
import os
import time
import unicodedata


def normalize_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace("\u2013", "-").replace("\u2014", "-")
    s = " ".join(s.split()).strip().lower()
    return s


# === Paths ===
base_dir = os.path.dirname(os.path.abspath(__file__))
input_csv = os.path.join(base_dir, "../../Data/Clubs/sofifa_clubs_clean.csv")
output_csv_raw = os.path.join(base_dir, "../../Data/Clubs/transfermarkt_dynamic_leagues.csv")
output_csv_sofifa_only = os.path.join(base_dir, "../../Data/Clubs/transfermarkt_clubs_sofifa_only.csv")

# === Load SoFIFA clubs ===
if not os.path.exists(input_csv):
    raise FileNotFoundError(f"❌ Input file not found: {input_csv}")

sofifa_df = pd.read_csv(input_csv)

# Ensure required columns (support fallback names)
if "club" not in sofifa_df.columns and "name" in sofifa_df.columns:
    sofifa_df = sofifa_df.rename(columns={"name": "club"})
if "league_name" not in sofifa_df.columns and "league" in sofifa_df.columns:
    sofifa_df = sofifa_df.rename(columns={"league": "league_name"})
if "country_name" not in sofifa_df.columns and "country" in sofifa_df.columns:
    sofifa_df = sofifa_df.rename(columns={"country": "country_name"})

for col in ["league_name", "club"]:
    if col not in sofifa_df.columns:
        raise KeyError(f"Missing '{col}' in SoFIFA CSV: {input_csv}")

print(f"✅ Loaded {len(sofifa_df)} clubs from SoFIFA CSV")

# Normalized key set for filtering
sofifa_df["_key"] = sofifa_df["league_name"].map(normalize_text) + "||" + sofifa_df["club"].map(normalize_text)
sofifa_keys = set(sofifa_df["_key"].tolist())

# === 1️⃣ Get unique leagues ===
unique_leagues = sorted(sofifa_df["league_name"].unique())
print(f"📊 Found {len(unique_leagues)} unique leagues in dataset")

# === 2️⃣ Mapping known SoFIFA league names → Transfermarkt URLs ===
# ✅ Working URLs
# ❌ Commented-out variants were tested and failed (kept for documentation)

league_map = {
    "Premier League": "https://www.transfermarkt.com/premier-league/startseite/wettbewerb/GB1",
    "EFL Championship": "https://www.transfermarkt.com/championship/startseite/wettbewerb/GB2",
    "EFL League One": "https://www.transfermarkt.com/league-one/startseite/wettbewerb/GB3",
    "EFL League Two": "https://www.transfermarkt.com/league-two/startseite/wettbewerb/GB4",

    "La Liga": "https://www.transfermarkt.com/primera-division/startseite/wettbewerb/ES1",
    # "https://www.transfermarkt.com/laliga/startseite/wettbewerb/ES1",

    "La Liga 2": "https://www.transfermarkt.com/laliga2/startseite/wettbewerb/ES2",
    "Serie A": "https://www.transfermarkt.com/serie-a/startseite/wettbewerb/IT1",
    "Serie B": "https://www.transfermarkt.com/serie-b/startseite/wettbewerb/IT2",
    "Bundesliga": "https://www.transfermarkt.com/bundesliga/startseite/wettbewerb/L1",
    "2. Bundesliga": "https://www.transfermarkt.com/2-bundesliga/startseite/wettbewerb/L2",
    "3. Liga": "https://www.transfermarkt.com/3-liga/startseite/wettbewerb/L3",

    "Ligue 1 Uber Eats": "https://www.transfermarkt.com/ligue-1/startseite/wettbewerb/FR1",
    "Ligue 2 BKT": "https://www.transfermarkt.com/ligue-2/startseite/wettbewerb/FR2",
    "Eredivisie": "https://www.transfermarkt.com/eredivisie/startseite/wettbewerb/NL1",
    "Primeira Liga": "https://www.transfermarkt.com/liga-portugal/startseite/wettbewerb/PO1",

    "Belgian Pro League": "https://www.transfermarkt.com/jupiler-pro-league/startseite/wettbewerb/BE1",
    "Turkish Süper Lig": "https://www.transfermarkt.com/super-lig/startseite/wettbewerb/TR1",
    "Swiss Super League": "https://www.transfermarkt.com/super-league/startseite/wettbewerb/C1",
    "Scottish Premiership": "https://www.transfermarkt.com/scottish-premiership/startseite/wettbewerb/SC1",
    "Greek Super League": "https://www.transfermarkt.com/super-league/startseite/wettbewerb/GR1",
    "A-League Men": "https://www.transfermarkt.com/a-league/startseite/wettbewerb/AUS1",
    "Major League Soccer": "https://www.transfermarkt.com/major-league-soccer/startseite/wettbewerb/MLS1",

    "Ekstraklasa": "https://www.transfermarkt.com/ekstraklasa/startseite/wettbewerb/PL1",
    "Superliga": "https://www.transfermarkt.com/superligaen/startseite/wettbewerb/DK1",
    "Allsvenskan": "https://www.transfermarkt.com/allsvenskan/startseite/wettbewerb/SE1",
    "Eliteserien": "https://www.transfermarkt.com/eliteserien/startseite/wettbewerb/NO1",
    "Liga I": "https://www.transfermarkt.com/liga-1/startseite/wettbewerb/RO1",
    "Czech First League": "https://www.transfermarkt.com/1-fotbalova-liga/startseite/wettbewerb/TS1",
    "Hrvatska nogometna liga": "https://www.transfermarkt.com/1-hnl/startseite/wettbewerb/KR1",
    "Nemzeti Bajnokság I": "https://www.transfermarkt.com/nb-i/startseite/wettbewerb/UNG1",
    "Indian Super League": "https://www.transfermarkt.com/indian-super-league/startseite/wettbewerb/IND1",
    "Saudi Pro League": "https://www.transfermarkt.com/saudi-professional-league/startseite/wettbewerb/SA1",
    "UAE Pro League": "https://www.transfermarkt.com/uae-pro-league/startseite/wettbewerb/UAE1",

    "Irish Premier Division": "https://www.transfermarkt.com/league-of-ireland-premier-division/startseite/wettbewerb/IR1",

    "Cypriot First Division": "https://www.transfermarkt.com/1-division/startseite/wettbewerb/ZYP1",

    "K League 1": "https://www.transfermarkt.com/k-league-1/startseite/wettbewerb/RSK1",
    "Veikkausliiga": "https://www.transfermarkt.com/veikkausliiga/startseite/wettbewerb/FI1",

    "Premyer Liqa": "https://www.transfermarkt.com/premyer-liqasi/startseite/wettbewerb/AZ1",

    "División de Fútbol Profesional": "https://www.transfermarkt.com/division-profesional/startseite/wettbewerb/BOL1",

    "Primera División": "https://www.transfermarkt.com/torneo-apertura/startseite/wettbewerb/ARG1",
}

# === Fallback URLs ===
fallback_urls = {
    "Primeira Liga": [
        "https://www.transfermarkt.com/primeira-liga/startseite/wettbewerb/PO1",
    ],
    "La Liga": [
        "https://www.transfermarkt.com/laliga/startseite/wettbewerb/ES1",
    ],
    "División de Fútbol Profesional": [
        "https://www.transfermarkt.fr/division-profesional-apertura/startseite/wettbewerb/BO1A",
    ],
    "Primera División": [
        "https://www.transfermarkt.fr/torneo-apertura/startseite/wettbewerb/ARG1",
    ],
    "Indian Super League": [
        "https://www.transfermarkt.com/super-league-india/startseite/wettbewerb/IND1",
        "https://www.transfermarkt.com/isl/startseite/wettbewerb/IND1",
    ],
    "Greek Super League": [
        "https://www.transfermarkt.fr/super-league/startseite/wettbewerb/GR1",
    ],
    "Saudi Pro League": [
        "https://www.transfermarkt.com/saudi-pro-league/startseite/wettbewerb/SA1",
        "https://www.transfermarkt.fr/saudi-professional-league/startseite/wettbewerb/SA1",
    ],
    "UAE Pro League": [
        "https://www.transfermarkt.com/arabian-gulf-league/startseite/wettbewerb/UAE1",
        "https://www.transfermarkt.fr/uae-pro-league/startseite/wettbewerb/UAE1",
    ],
}


# === Helper ===
def clean_money(val):
    """Convert text like '€1.15bn' or '€52.3m' to numeric euro value."""
    if not isinstance(val, str) or val.strip() == "":
        return None
    val = val.replace("€", "").replace(",", "").strip().lower()
    multiplier = 1
    if "bn" in val:
        val = val.replace("bn", "")
        multiplier = 1_000_000_000
    elif "m" in val:
        val = val.replace("m", "")
        multiplier = 1_000_000
    elif "k" in val:
        val = val.replace("k", "")
        multiplier = 1_000
    try:
        return float(val) * multiplier
    except Exception:
        return None


def get_candidate_urls(league_name: str):
    """Return a list of candidate URLs for a league (primary + fallbacks).
    Also handles ambiguous generic names like 'Super League' by inspecting countries in SoFIFA.
    """
    candidates = []
    primary = league_map.get(league_name)
    if primary:
        candidates.append(primary)
    for alt in fallback_urls.get(league_name, []):
        if alt not in candidates:
            candidates.append(alt)

    # Special-case generic 'Super League': infer by country
    if league_name == "Super League":
        try:
            countries = (
                sofifa_df.loc[sofifa_df["league_name"] == "Super League", "country_name"]
                .dropna()
                .astype(str)
                .str.strip()
                .str.lower()
                .unique()
                .tolist()
            )
        except Exception:
            countries = []
        if "switzerland" in countries:
            url = "https://www.transfermarkt.com/super-league/startseite/wettbewerb/C1"
            if url not in candidates:
                candidates.append(url)
        if "china" in countries or "china pr" in countries:
            for url in [
                "https://www.transfermarkt.com/chinese-super-league/startseite/wettbewerb/CSL",
                "https://www.transfermarkt.fr/chinese-super-league/startseite/wettbewerb/CSL",
            ]:
                if url not in candidates:
                    candidates.append(url)
    return candidates


# === 3️⃣ Scraping ===
results = []

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
    context = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/125.0 Safari/537.36",
        viewport={"width": 1280, "height": 900}
    )
    page = context.new_page()

    def accept_cookies_if_present():
        try:
            page.locator("button#onetrust-accept-btn-handler").click(timeout=3000)
            time.sleep(1)
        except Exception:
            pass

    for league in unique_leagues:
        candidates = get_candidate_urls(league)
        if not candidates:
            print(f"⏭️ Skipping unrecognized league: {league}")
            continue

        league_collected = []
        last_error = None

        for idx, url in enumerate(candidates):
            print(f"\n🏆 Scraping {league} from {url} (candidate {idx+1}/{len(candidates)})")

            # adaptive timeout
            timeout_val = 90000 if league in [
                "EFL Championship", "Primeira Liga", "Major League Soccer",
                "Primera División", "Scottish Premiership"
            ] else 60000

            for attempt in range(2):  # retry up to twice
                try:
                    page.goto(url, timeout=timeout_val, wait_until="networkidle")
                    accept_cookies_if_present()

                    # give JS a few extra seconds
                    page.wait_for_selector("table.items tbody tr", timeout=40000)
                    rows = page.query_selector_all("table.items tbody tr")
                    if not rows:
                        print("⚠️ No table rows found after render — retrying once...")
                        time.sleep(5)
                        continue

                    candidate_results = []
                    for row in rows:
                        try:
                            link = row.query_selector("td.hauptlink a")
                            if not link:
                                continue
                            href = link.get_attribute("href") or ""
                            if "/verein/" not in href:
                                continue
                            club_name = link.inner_text().strip()
                            # ensure absolute URL
                            club_url = href if href.startswith("http") else f"https://www.transfermarkt.com{href}"
                            # extract numeric club id from href
                            club_id = None
                            try:
                                import re
                                m = re.search(r"/verein/(\d+)", href)
                                if m:
                                    club_id = int(m.group(1))
                            except Exception:
                                club_id = None

                            rechts = row.query_selector_all("td.rechts")
                            if len(rechts) < 2:
                                continue

                            avg_value = rechts[-2].inner_text().strip()
                            total_value = rechts[-1].inner_text().strip()

                            def norm(v):
                                if not v:
                                    return None
                                v = v.replace("\u2009", " ").replace("\xa0", " ").strip()
                                return None if v in {"-", "—", "", "N/A"} else v

                            avg_value = norm(avg_value)
                            total_value = norm(total_value)

                            if total_value is None and avg_value is None:
                                continue

                            candidate_results.append({
                                "league_name": league,
                                "club": club_name,
                                "club_id": club_id,
                                "club_url": club_url,
                                "average_market_value": avg_value,
                                "total_market_value": total_value,
                                "average_market_value_eur": clean_money(avg_value),
                                "total_market_value_eur": clean_money(total_value),
                                "source_url": url
                            })
                        except Exception:
                            continue

                    if candidate_results:
                        print(f"✅ {league}: collected {len(candidate_results)} clubs successfully")
                        league_collected = candidate_results
                        break  # stop retry loop

                except Exception as e:
                    last_error = e
                    print(f"⚠️ Attempt {attempt+1} failed: {e}")
                    time.sleep(5)
                    if attempt == 0:
                        print("🔁 Retrying once more...")
                        continue
                    else:
                        print("❌ Both attempts failed.")
                break  # exit after success or both fails

            if league_collected:
                break

        if not league_collected:
            print(f"❌ {league}: No data collected | last error: {last_error}")
            continue

        results.extend(league_collected)
        time.sleep(3)

    browser.close()

# === 4️⃣ Save results ===
df_tm = pd.DataFrame(results)
if not df_tm.empty:
    df_tm["club"] = df_tm["club"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    df_tm = df_tm.drop_duplicates(subset=["league_name", "club"], keep="last").reset_index(drop=True)

# Save raw scrape
df_tm.to_csv(output_csv_raw, index=False, encoding="utf-8-sig")

# === 5️⃣ Filter to SoFIFA clubs only ===
if not df_tm.empty:
    df_tm["_key"] = df_tm["league_name"].map(normalize_text) + "||" + df_tm["club"].map(normalize_text)
    df_tm["in_sofifa"] = df_tm["_key"].isin(sofifa_keys)
    df_sofifa_only = df_tm[df_tm["in_sofifa"]].drop(columns=["_key", "in_sofifa"]).reset_index(drop=True)
else:
    df_sofifa_only = df_tm.copy()

df_sofifa_only.to_csv(output_csv_sofifa_only, index=False, encoding="utf-8-sig")

print(f"\n💾 Saved raw: {len(df_tm)} rows → {output_csv_raw}")
print(f"💾 Saved SoFIFA-only: {len(df_sofifa_only)} rows → {output_csv_sofifa_only}")