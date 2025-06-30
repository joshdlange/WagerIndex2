import os
import datetime
from supabase import create_client
from pybaseball import team_batting, team_pitching
import pandas as pd

# Supabase setup
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Determine season year
season_year = datetime.date.today().year

# Load team batting & pitching stats
print(f"📊 Fetching team batting and pitching stats for {season_year}")
batting_df = team_batting(season_year)
pitching_df = team_pitching(season_year)

# Merge and clean up
merged_df = pd.merge(batting_df, pitching_df, on="Team", suffixes=("_bat", "_pitch"))

# Rename and select columns you need — adjust based on your Supabase table schema
mapped_df = pd.DataFrame({
    "team_name": merged_df["Team"],
    "runs_per_game": merged_df["R/G"],
    "batting_avg": merged_df["AVG_bat"],
    "on_base_pct": merged_df["OBP_bat"],
    "slugging_pct": merged_df["SLG_bat"],
    "ops": merged_df["OPS_bat"],
    "era": merged_df["ERA_pitch"],
    "whip": merged_df["WHIP_pitch"],
    "strikeouts_per_9": merged_df["SO9_pitch"],
    "walks_per_9": merged_df["BB9_pitch"]
}).dropna()

# Upload each row to Supabase
print(f"⬆️ Uploading team stats to Supabase ({len(mapped_df)} teams)")
for _, row in mapped_df.iterrows():
    data = row.to_dict()
    try:
        supabase.table("team_stats").upsert(data).execute()
    except Exception as e:
        print(f"❌ Error inserting team {data['team_name']}: {e}")

print("✅ Done syncing team stats.")
