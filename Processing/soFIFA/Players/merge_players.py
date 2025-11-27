import os
import pandas as pd
import unidecode
import re
from rapidfuzz import fuzz, process

# --------------------------
# 1. Helper: Normalize names
# --------------------------
def normalize_name(name):
    if pd.isna(name):
        return ""
    name = name.lower()
    name = unidecode.unidecode(name)          # remove accents
    name = re.sub(r"[^a-z ]", " ", name)      # remove symbols
    name = re.sub(r"\s+", " ", name).strip()  # clean spaces
    return name


# 2. Resolve paths and load datasets (root-level Data)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", "..", ".."))

sofifa_path = os.path.join(REPO_ROOT, "Data", "soFIFA", "Players", "player_stats_cleaned_light.csv")

# Prefer filtered players; fallback to raw if present under expected names
tm_candidates = [
    os.path.join(REPO_ROOT, "Data", "Transfermarkt", "Players", "transfermarkt_players.csv"),
    os.path.join(REPO_ROOT, "Data", "Transfermarkt", "Players", "players_from_365.filtered.csv"),
    os.path.join(REPO_ROOT, "Data", "Transfermarkt", "Players", "players_from_365.csv"),
]
tm_path = next((p for p in tm_candidates if os.path.exists(p)), None)

if not os.path.exists(sofifa_path):
    raise FileNotFoundError(f"SoFIFA cleaned light CSV not found at: {sofifa_path}")
if not tm_path:
    raise FileNotFoundError("No Transfermarkt players CSV found. Expected one of: " + ", ".join(tm_candidates))

# Use utf-8-sig to match our writing convention
sofifa = pd.read_csv(sofifa_path, encoding="utf-8-sig")
tm = pd.read_csv(tm_path, encoding="utf-8-sig")


# 3. Normalize names

sofifa["std_name"] = sofifa["full_name"].apply(normalize_name)
tm["std_name"] = tm["player_name"].apply(normalize_name)


# 4. Exact match

exact_matches = sofifa.merge(tm, on="std_name", how="inner", suffixes=("_sofifa", "_tm"))

matched_sofifa_names = set(exact_matches["std_name"])
unmatched_sofifa = sofifa[~sofifa["std_name"].isin(matched_sofifa_names)].copy()

print(f"Exact matches: {len(exact_matches)}")
print(f"Remaining unmatched for fuzzy: {len(unmatched_sofifa)}")


# 5. Prepare fuzzy matching

tm_names = tm["std_name"].tolist()

fuzzy_results = []
unresolved = []

for _, row in unmatched_sofifa.iterrows():
    name = row["std_name"]

    # Find best fuzzy match
    best_match, score, idx = process.extractOne(
        name,
        tm_names,
        scorer=fuzz.token_sort_ratio
    )

    if score >= 80:  # threshold
        tm_row = tm.iloc[idx]
        merged = {**row.to_dict(), **{f"tm_{col}": tm_row[col] for col in tm.columns}}
        fuzzy_results.append(merged)
    else:
        unresolved.append(row.to_dict())

# Convert lists to DF
fuzzy_df = pd.DataFrame(fuzzy_results)
unresolved_df = pd.DataFrame(unresolved)

print(f"Fuzzy matched: {len(fuzzy_df)}")
print(f"Unresolved: {len(unresolved_df)}")

# 6. Final merged dataset

final_merged = pd.concat([exact_matches, fuzzy_df], ignore_index=True)

# --------------------------
# 7. Save outputs
# --------------------------
merged_dir = os.path.join(REPO_ROOT, "Data", "Merged")
os.makedirs(merged_dir, exist_ok=True)
final_merged.to_csv(os.path.join(merged_dir, "merged_players.csv"), index=False, encoding="utf-8-sig")
unresolved_df.to_csv(os.path.join(merged_dir, "unresolved_players.csv"), index=False, encoding="utf-8-sig")

print("Done! Files saved in Data/Merged/")
