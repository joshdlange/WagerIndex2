# fetch_team_stats.py
import os
import sys
import pandas as pd
import numpy as np
from supabase import create_client, Client
from sportsipy.mlb.teams import Teams
from dotenv import load_dotenv
from datetime import datetime
from utils import get_current_season_year # <-- IMPORT THE FIX

load_dotenv()

# --- Config ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def fetch_and_upsert_team_stats():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Fatal Error: SUPABASE_URL and SUPABASE_KEY secrets must be set.")
        sys.exit(1)

    SEASON_YEAR = get_current_season_year() # <-- USE THE FIX
    print(f"📊 Fetching team stats for the {SEASON_YEAR} season using sportsipy...")

    try:
        teams = Teams(SEASON_YEAR)
        if not teams or len(list(teams)) == 0:
            print(f"❌ Error: No team stats data found for {SEASON_YEAR}. Aborting.")
            sys.exit(1)
        teams_df = teams.dataframes
    except Exception as e:
        print(f"❌ Error fetching data from sportsipy for {SEASON_YEAR}: {e}")
        sys.exit(1)

    # Sanitize data to be JSON compliant
    teams_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    records_df = teams_df.where(pd.notnull(teams_df), None)

    records = []
    for _, team in records_df.iterrows():
        records.append({
            "team_abbr": team["abbreviation"], "team_name": team["name"],
            "games_played": team.get("games"), "runs": team.get("runs"), "hits": team.get("hits"),
            "home_runs": team.get("home_runs"), "batting_average": team.get("batting_average"),
            "strikeouts_batting": team.get("strikeouts"), "walks_batting": team.get("walks"),
            "era": team.get("earned_run_average"), "whip": team.get("walks_hits_per_inning_pitched"),
            "errors": team.get("errors"), "last_updated": datetime.now().isoformat()
        })
    
    print(f"⬆️ Upserting {len(records)} teams' stats into Supabase...")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        response = supabase.table("team_stats").upsert(records, on_conflict="team_abbr").execute()
        if response.data:
            print("✅ Team stats upserted successfully.")
        else:
            print(f"❌ Supabase Error: {getattr(response, 'error', 'Unknown error')}")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Supabase upsert failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_and_upsert_team_stats()
