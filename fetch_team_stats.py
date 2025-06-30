import os
import uuid
from datetime import date
from supabase import create_client
from pybaseball import team_batting, team_pitching
import pandas as pd

# Get Supabase credentials from environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_and_insert_team_stats():
    print("📊 Fetching team batting and pitching stats for current season...")
    season = date.today().year

    # Fetch stats from pybaseball
    batting_df = team_batting(season)
    pitching_df = team_pitching(season)

    # Standardize team names
    batting_df["Team"] = batting_df["Team"].str.replace("*", "", regex=False)
    pitching_df["Team"] = pitching_df["Team"].str.replace("*", "", regex=False)

    # Merge batting and pitching stats
    merged_df = pd.merge(batting_df, pitching_df, on="Team", suffixes=("_bat", "_pit"))

    # Compute runs per game (manually: R / G)
    merged_df["runs_per_game"] = merged_df["R_bat"] / merged_df["G_bat"]

    # Select and rename relevant columns
    output_df = merged_df[["Team", "ERA", "runs_per_game"]].copy()
    output_df.rename(columns={"Team": "team_name", "ERA": "era"}, inplace=True)

    print("🆙 Inserting team stats into Supabase...")
    for _, row in output_df.iterrows():
        result = supabase.table("team_stats").insert({
            "id": str(uuid.uuid4()),
            "team_name": row["team_name"],
            "era": round(row["era"], 2) if pd.notna(row["era"]) else None,
            "runs_per_game": round(row["runs_per_game"], 2) if pd.notna(row["runs_per_game"]) else None,
            "updated_at": date.today().isoformat()
        }).execute()
        if result.error:
            print(f"❌ Error inserting {row['team_name']}: {result.error}")
        else:
            print(f"✅ Inserted {row['team_name']}")

if __name__ == "__main__":
    fetch_and_insert_team_stats()
