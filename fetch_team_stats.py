# fetch_team_stats.py
import os
import sys
import requests
import pandas as pd
import numpy as np
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime
from utils import get_current_season_year

load_dotenv()

# --- Config ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def fetch_and_upsert_team_stats():
    """
    Fetches comprehensive team stats directly from the ESPN API,
    which is the same source as their public website, ensuring data availability.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Fatal Error: SUPABASE_URL and SUPABASE_KEY secrets must be set.")
        sys.exit(1)

    try:
        # Use our robust utility to get the correct season year
        SEASON_YEAR = get_current_season_year()
    except Exception as e:
        sys.exit(1) # Exit if we can't even get the year

    # This is the API endpoint that powers the ESPN stats website
    STATS_API_URL = f"https://site.api.espn.com/apis/v2/sports/baseball/mlb/seasons/{SEASON_YEAR}/types/2/teams?lang=en®ion=us&contentorigin=espn&limit=50"
    
    print(f"📊 Fetching team stats for the {SEASON_YEAR} season directly from ESPN API...")

    try:
        response = requests.get(STATS_API_URL)
        response.raise_for_status()
        data = response.json()
        
        if not data.get("items"):
            print(f"❌ Error: ESPN API returned no team stat items for {SEASON_YEAR}. Aborting.")
            sys.exit(1)

        teams_data = data["items"]
    except Exception as e:
        print(f"❌ Error fetching or parsing data from ESPN Stats API: {e}")
        sys.exit(1)

    records = []
    print("  - Processing each team...")
    for item in teams_data:
        team_info = item.get("team", {})
        stats_map = {s["name"]: s["value"] for s in team_info.get("stats", [])}

        # Create a record for the team, handling potentially missing stats
        team_record = {
            "team_abbr": team_info.get("abbreviation"),
            "team_name": team_info.get("displayName"),
            "games_played": stats_map.get("gamesPlayed"),
            "runs": stats_map.get("runs"),
            "hits": stats_map.get("hits"),
            "home_runs": stats_map.get("homeRuns"),
            "batting_average": stats_map.get("avg"),
            "strikeouts_batting": stats_map.get("strikeouts"),
            "walks_batting": stats_map.get("walks"),
            "era": stats_map.get("earnedRunAverage"),
            "whip": stats_map.get("walksAndHitsPerInningPitched"),
            "errors": stats_map.get("errors"),
            "last_updated": datetime.now().isoformat()
        }
        records.append(team_record)

    if not records:
        print("❌ No team records were successfully processed. Aborting Supabase upload.")
        sys.exit(1)
        
    # --- Data Sanitization (Best Practice) ---
    df = pd.DataFrame(records)
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    final_records = df.where(pd.notnull(df), None).to_dict('records')

    print(f"⬆️ Upserting {len(final_records)} teams' complete stats into Supabase...")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        response = supabase.table("team_stats").upsert(final_records, on_conflict="team_abbr").execute()
        if response.data:
            print("✅ Team stats upserted successfully.")
        else:
            print(f"❌ Supabase Error: {getattr(response, 'error', 'Unknown error')}")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Supabase upsert failed with an exception: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_and_upsert_team_stats()
