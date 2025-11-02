from playwright.sync_api import sync_playwright
import pandas as pd
import os
import re
import time
import argparse


base_dir = os.path.dirname(os.path.abspath(__file__))
clubs_csv = os.path.join(base_dir, "../../Data/Clubs/transfermarkt_clubs_sofifa_only.csv")
out_dir = os.path.join(base_dir, "../../Data/Transfermarkt")
out_csv_raw = os.path.join(out_dir, "players_from_365.csv")
out_csv_min = os.path.join(out_dir, "players_from_365.filtered.csv")


def ensure_abs(url: str) -> str:
	if not isinstance(url, str):
		return ""
	return url if url.startswith("http") else f"https://www.transfermarkt.com{url}"


def money_to_eur(val: str):
	if not isinstance(val, str) or not val.strip():
		return None
	v = val.replace("€", "").replace(",", "").strip().lower()
	mult = 1
	if v.endswith("bn"):
		v = v[:-2]
		mult = 1_000_000_000
	elif v.endswith("m"):
		v = v[:-1]
		mult = 1_000_000
	elif v.endswith("k"):
		v = v[:-1]
		mult = 1_000
	try:
		return float(v) * mult
	except Exception:
		return None


def find_squad_url(page, club_id: int | None):
	try:
		a = page.query_selector('a[href*="/kader/verein/"]')
		if a:
			href = a.get_attribute("href") or ""
			if href:
				return ensure_abs(href)
	except Exception:
		pass
	if club_id:
		candidates = [
			f"https://www.transfermarkt.com/kader/verein/{club_id}/plus/1",
			f"https://www.transfermarkt.com/kader/verein/{club_id}/saison_id/2025/plus/1",
			f"https://www.transfermarkt.com/kader/verein/{club_id}/saison_id/2024/plus/1",
		]
		return candidates  # caller will try them
	return None


def scrape_players_for_club(page, league_name: str, club_name: str, club_url: str, club_id: int | None):
	records = []
	# navigate to club page
	page.goto(club_url, timeout=60000, wait_until="networkidle")
	# accept cookies if present
	try:
		page.locator("button#onetrust-accept-btn-handler").click(timeout=2000)
		time.sleep(0.5)
	except Exception:
		pass

	squad = find_squad_url(page, club_id)
	squad_url = None
	if isinstance(squad, str):
		squad_url = squad
	elif isinstance(squad, list):
		# try candidates
		for cu in squad:
			try:
				page.goto(cu, timeout=60000, wait_until="networkidle")
				if page.query_selector("table.items tbody tr"):
					squad_url = cu
					break
			except Exception:
				continue

	if not squad_url:
		# last attempt: stay on club page and hope table exists
		squad_url = page.url

	if page.url != squad_url:
		page.goto(squad_url, timeout=60000, wait_until="networkidle")

	try:
		page.wait_for_selector("table.items tbody tr", timeout=40000)
	except Exception:
		return records

	rows = page.query_selector_all("table.items tbody tr")
	count = 0
	for tr in rows:
		try:
			a = tr.query_selector("td.hauptlink a")
			if not a:
				continue
			href = a.get_attribute("href") or ""
			if "/spieler/" not in href:
				continue
			player_url = ensure_abs(href)
			m = re.search(r"/spieler/(\d+)", href)
			player_id = int(m.group(1)) if m else None
			player_name = (a.inner_text() or "").strip()

			# Optional fields
			pos = None
			try:
				pos_cell = tr.query_selector("td:nth-child(2)")
				if pos_cell:
					pos = (pos_cell.inner_text() or "").strip()
			except Exception:
				pass

			age = None
			try:
				cells = tr.query_selector_all("td.zentriert")
				for c in cells:
					txt = (c.inner_text() or "").strip()
					if txt.isdigit() and 14 <= int(txt) <= 48:
						age = int(txt)
						break
			except Exception:
				pass

			nat = None
			try:
				flags = tr.query_selector_all("td.zentriert img[title]")
				if flags:
					nat = "|".join(sorted({(f.get_attribute("title") or "").strip() for f in flags if (f.get_attribute("title") or "").strip()}))
			except Exception:
				pass

			mv = None
			try:
				rechts = tr.query_selector_all("td.rechts")
				if rechts:
					mv = (rechts[-1].inner_text() or "").strip()
					mv = None if mv in {"-","—","","N/A"} else mv
			except Exception:
				pass

			records.append({
				"league_name": league_name,
				"club": club_name,
				"club_id": club_id,
				"player_name": player_name,
				"player_id": player_id,
				"player_url": player_url,
				"position": pos,
				"age": age,
				"nationality": nat,
				"market_value": mv,
				"market_value_eur": money_to_eur(mv),
				"squad_url": squad_url,
			})
			count += 1
		except Exception:
			continue
	print(f"✅ {club_name}: collected {count} players")
	return records


def main():
	ap = argparse.ArgumentParser()
	ap.add_argument("--limit", type=int, default=None, help="Limit number of clubs to process")
	args = ap.parse_args()

	if not os.path.exists(clubs_csv):
		raise FileNotFoundError(f"Input clubs CSV not found: {clubs_csv}")

	clubs = pd.read_csv(clubs_csv)
	needed = {"league_name", "club", "club_url", "club_id"}
	missing = needed - set(clubs.columns)
	if missing:
		raise KeyError(f"Missing columns in clubs CSV: {missing}. Re-run transfermarkt_clubs.py to include club_url/club_id.")

	if args.limit:
		clubs = clubs.head(args.limit)

	os.makedirs(out_dir, exist_ok=True)

	all_rows = []
	with sync_playwright() as p:
		browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
		context = browser.new_context(
			user_agent=(
				"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
				"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
			),
			viewport={"width": 1280, "height": 900},
		)
		page = context.new_page()

		for _, row in clubs.iterrows():
			league_name = str(row.get("league_name", "")).strip()
			club_name = str(row.get("club", "")).strip()
			club_url = ensure_abs(str(row.get("club_url", "")).strip())
			club_id = int(row.get("club_id")) if pd.notna(row.get("club_id")) else None
			if not club_url:
				print(f"⏭️ Skipping (no club_url): {league_name} - {club_name}")
				continue
			print(f"\n👥 {league_name} — {club_name} | {club_url}")
			try:
				recs = scrape_players_for_club(page, league_name, club_name, club_url, club_id)
				all_rows.extend(recs)
			except KeyboardInterrupt:
				print("Interrupted. Saving partial results...")
				break
			except Exception as e:
				print(f"⚠️ Error on {club_name}: {e}")
				continue

		browser.close()

	df = pd.DataFrame(all_rows)
	df.to_csv(out_csv_raw, index=False, encoding="utf-8-sig")
	print(f"\n💾 Saved players: {len(df)} rows → {out_csv_raw}")

	# Minimal, deduplicated file
	if not df.empty:
		cols = [
			"league_name","club","club_id","player_name","player_id","player_url",
			"position","age","nationality","market_value","market_value_eur"
		]
		keep = [c for c in cols if c in df.columns]
		df_min = df[keep].drop_duplicates(subset=["club_id","player_id","player_name"], keep="last").reset_index(drop=True)
		df_min.to_csv(out_csv_min, index=False, encoding="utf-8-sig")
		print(f"💾 Saved players (filtered): {len(df_min)} rows → {out_csv_min}")


if __name__ == "__main__":
	main()

