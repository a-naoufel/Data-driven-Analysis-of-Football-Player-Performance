import pandas as pd
import sys
import os

def clean_country_kit_number(df, col="country_kit_number"):
    """
    Ensures the column contains only integers.
    Non-numeric or missing values are replaced with 0.
    """
    if col not in df.columns:
        print(f"❌ Error: Column '{col}' not found in DataFrame.")
        return df

    # Convert to numeric, forcing errors to NaN
    df[col] = pd.to_numeric(df[col], errors="coerce")

    # Replace NaN with 0
    df[col] = df[col].fillna(0).astype(int)

    print(f"✅ Cleaned '{col}' — all values are now integers (empty → 0).")
    return df




def add_dob_parts(df, dob_col="dob"):
    """
    Adds 'yearob' and 'monthob' columns based on a DOB column in 'YYYY-MM-DD' format.
    """
    if dob_col not in df.columns:
        print(f"❌ Error: Column '{dob_col}' not found in DataFrame.")
        return df

    # Convert DOB column to datetime
    df[dob_col] = pd.to_datetime(df[dob_col], errors='coerce')

    # Create new columns
    df["yearob"] = df[dob_col].dt.year
    df["monthob"] = df[dob_col].dt.month

    print("✅ Added columns: 'yearob' and 'monthob'")
    return df

def add_date_parts(df, dob_col="club_joined"):
    """
    Adds 'yearob' and 'monthob' columns based on a DOB column.
    Works with both 'YYYY-MM-DD' and 'MMM D, YYYY' formats.
    """
    if dob_col not in df.columns:
        print(f"❌ Error: Column '{dob_col}' not found in DataFrame.")
        return df

    # Convert DOB column to datetime (auto-detects format)
    df[dob_col] = pd.to_datetime(df[dob_col], errors='coerce', infer_datetime_format=True)

    # Create new columns
    df["year_club_joined"] = df[dob_col].dt.year
    df["month_club_joined"] = df[dob_col].dt.month

    print("✅ Added columns: 'yearob' and 'monthob'")
    return df

import pandas as pd

def expand_positions(df, col="positions"):
    """
    Expands a comma-separated 'positions' column (e.g. 'ST, LW, LM')
    into one-hot encoded columns for each unique position.
    """
    print("🔍 Expanding positions...")
    if col not in df.columns:
        print(f"❌ Error: Column '{col}' not found in DataFrame.")
        return df

    # Ensure all values are strings (avoid NaN issues)
    df[col] = df[col].fillna("").astype(str)

    # One-hot encode positions
    positions_dummies = df[col].str.get_dummies(sep=",").rename(columns=lambda x: x.strip())

    # Merge encoded columns back into DataFrame
    df = pd.concat([df, positions_dummies], axis=1)

    print(f"✅ Added {len(positions_dummies.columns)} position columns:")
    print(", ".join(positions_dummies.columns))
    return df



def clean_csv(file_path):
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"❌ Error: File '{file_path}' not found.")
        return

    # Read CSV
    df = pd.read_csv(file_path)
    cols_to_drop = [
        'play_styles','specialities','positions','club_joined','dob','club_league_name','club_league_name','club_league_id',
        'skill_moves','image','player_id','real_face','release_clause','country_rating',
        'handedness','club_logo','club_rating','club_position','country_flag',''
        'attacking_crossing','photo_url','description','overall_rating','potential'
        ,'country_id','country_league_id','country_rating','country_position','attacking_crossing'
        ,'attacking_finishing','attacking_heading_accuracy','attacking_short_passing',
        'attacking_volleys','skill_dribbling','skill_curve','skill_fk_accuracy',
        'skill_long_passing','skill_ball_control','movement_acceleration','movement_sprint_speed'
        ,'movement_agility','movement_reactions','movement_balance','power_shot_power','power_jumping'
        ,'power_stamina','power_strength','power_long_shots','mentality_aggression','mentality_interceptions'
        ,'mentality_vision','mentality_penalties','mentality_composure','defending_defensive_awareness',
        'defending_standing_tackle','defending_sliding_tackle','goalkeeping_gk_diving','goalkeeping_gk_handling',
        'goalkeeping_gk_kicking','goalkeeping_gk_positioning','goalkeeping_gk_reflexes','url','mentality_attack_position'
    ]
    # --- 1️⃣ Drop completely empty columns ---
    clean_country_kit_number(df, col="country_kit_number")
    add_dob_parts(df, dob_col="dob")
    add_date_parts(df, dob_col="club_joined")
    df = expand_positions(df, col="positions")
    df = expand_positions(df, col="specialities")
    df = expand_positions(df, col="play_styles")


    df = df.dropna(axis=1, how='all')

    # --- 2️⃣ Drop    columns with only one unique value ---
    df = df.loc[:, df.nunique() > 1]

    # --- 3️⃣ Drop columns with more than 50% missing values ---
    threshold = 0.5
    df = df.dropna(axis=1, thresh=int((1 - threshold) * len(df)))

    existing_cols = [col for col in cols_to_drop if col in df.columns]
    if not existing_cols:
        print(f"⚠️ None of the specified columns were found in {file_path}")
    else:
        df = df.drop(columns=existing_cols)
        print(f"🗑️ Dropped columns: {', '.join(existing_cols)}")

    # # --- 4️⃣ Clean column names ---
    # df.columns = df.columns.str.strip()

    # --- 5️⃣ Save cleaned file ---
    output_file = os.path.splitext(file_path)[0] + "_cleaned.csv"
    df.to_csv(output_file, index=False)

    print("✅ Cleaning complete!")
    print(f"💾 Saved cleaned file as: {output_file}")
    print("📊 Remaining columns:", ", ".join(df.columns))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python clean_player_data.py <csv_file>")
    else:
        clean_csv(sys.argv[1])
