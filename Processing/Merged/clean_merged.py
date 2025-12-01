import os
import pandas as pd
import numpy as np

# ------------------------------
# 1. Resolve paths
# ------------------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))

input_path = os.path.join(REPO_ROOT, "Data", "Merged", "merged_players.csv")
output_path = os.path.join(REPO_ROOT, "Data", "Merged", "merged_players_clean.csv")

if not os.path.exists(input_path):
    raise FileNotFoundError(f"merged_players.csv not found at: {input_path}")

df = pd.read_csv(input_path, encoding="utf-8-sig")
print("Loaded merged dataset:", df.shape)

# ------------------------------
# 2. Remove players with missing market value
# ------------------------------
if "market_value_eur" in df.columns:
    market_col = "market_value_eur"
else:
    raise ValueError("No market value column found in merged dataset.")

initial_rows = len(df)
df = df[df[market_col].notna()]
print(f"Removed {initial_rows - len(df)} players with missing market value.")

# ------------------------------
# 3. Drop useless columns
# ------------------------------
cols_to_drop = [
    "squad_url", "player_id", "club_id", 
    "tm_league_name", "tm_club", "tm_club_id",
    "tm_player_name", "tm_player_id", "tm_player_url",
    "tm_position", "tm_age", "tm_nationality",
    "tm_market_value", "tm_market_value_eur", "tm_squad_url",
    "tm_std_name", "country_kit_number", "club_rating",
    "value", "wage", "name", "club_id_sofifa"
]

cols_to_drop = [c for c in cols_to_drop if c in df.columns]
df.drop(columns=cols_to_drop, inplace=True)
print("Dropped:", cols_to_drop)

# ------------------------------
# 4. Handle missing values
# ------------------------------

# --- 4A. Fill categorical missing → "Unknown"
categorical_cols = df.select_dtypes(include=["object"]).columns.tolist()
for col in categorical_cols:
    df[col] = df[col].fillna("Unknown")   # <- FIXED

# --- 4B. Fill numeric missing → median
numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
for col in numeric_cols:
    df[col] = df[col].fillna(df[col].median())  # <- FIXED

print("Handled missing values for categorical and numeric columns.")

# ------------------------------
# 5. Save cleaned dataset
# ------------------------------
df.to_csv(output_path, index=False, encoding="utf-8-sig")
print("Saved cleaned dataset to:", output_path)
print("Final shape:", df.shape)
