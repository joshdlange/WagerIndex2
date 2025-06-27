import os
from dotenv import load_dotenv
from supabase import create_client, Client
import pandas as pd
from pybaseball import team_batting, team_pitching

# Load Supabase credentials from .env
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Fetch latest season team batting stats
batting_df = team_batting(start_season=2024)

# Filter down to required batting stats
batting_df = batting_df[["Team", "R", "G"]].rename(columns={"Team": "team_abbr"})
batting_df["runs_per_game"] = batting_df["R"] / batting_df["G"]
batting_df = batting_df[["team_abbr", "runs_per_game"]]

# Fetch latest season team pitching stats
pitching_df = team_pitching(start_season=2024)
pitching_df = pitching_df[["Team", "ERA", "WHIP", "K/9", "BB/9"]].rename(columns={
    "Team": "team_abbr",
    "ERA": "era",
    "WHIP": "whip",
    "K/9": "k9",
    "BB/9": "bb9"
})

# Merge batting + pitching
merged = pd.merge(batting_df, pitching_df, on="team_abbr", how="inner")
print("\n📊 Preview of merged data:")
print(merged.head())

# Upsert each record into Supabase 'team_stats'
for _, row in merged.iterrows():
    record = row.to_dict()
    print(f"\n📤 Inserting: {record}")
    try:
        supabase.table("team_stats").upsert(record).execute()
    except Exception as e:
        print(f"❌ Failed to upsert {record['team_abbr']}: {e}")

print("\n✅ Team stats update complete.")
