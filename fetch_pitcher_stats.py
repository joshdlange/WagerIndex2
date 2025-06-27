import os
import pandas as pd
from pybaseball import pitching_stats
from supabase import create_client, Client
from dotenv import load_dotenv

# Load Supabase credentials from .env file
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Define the season (modify this as needed)
SEASON = 2024

# Fetch pitcher stats for the season
print("🔁 Fetching pitcher stats...")
df = pitching_stats(SEASON)

# Filter relevant columns
df = df[["Name", "Team", "IP", "ERA", "WHIP", "SO9", "BB9"]].copy()
df.dropna(subset=["Team"], inplace=True)

# Group by team and average out values per team (if needed)
df = df.groupby("Team").mean(numeric_only=True).reset_index()

# Rename columns for Supabase
stat_rows = []
for _, row in df.iterrows():
    team_abbr = row["Team"]
    stat_rows.append({
        "team_abbr": team_abbr,
        "avg_ip": round(row["IP"], 2),
        "avg_era": round(row["ERA"], 2),
        "avg_whip": round(row["WHIP"], 2),
        "avg_k9": round(row["SO9"], 2),
        "avg_bb9": round(row["BB9"], 2)
    })

# Upsert to Supabase
print(f"📤 Upserting {len(stat_rows)} pitcher stat records to Supabase...")
for record in stat_rows:
    try:
        response = supabase.table("pitchers").upsert(record).execute()
    except Exception as e:
        print(f"❌ Failed to upsert {record['team_abbr']}: {e}")

print("✅ Done fetching and uploading pitcher stats.")
