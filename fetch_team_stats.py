# fetch_team_stats.py
import os
import pandas as pd
from supabase import create_client, Client
from sportsipy.mlb.teams import Teams
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# --- Config ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
YEAR = datetime.now().year

def fetch_and_upsert_team_stats():
    """
    Fetches comprehensive team stats using the robust sportsipy library
    and upserts the data into Supabase. This is the single source for all team data.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Fatal Error: SUPABASE_URL and SUPABASE_KEY must be set in your .env file.")
        return

    print(f"📊 Fetching team stats for {YEAR} using sportsipy...")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        teams = Teams(YEAR)
    except Exception as e:
        print(f"❌ Failed to initialize sportsipy Teams or connect to Supabase: {e}")
        return

    records = []
    print("  - Processing each team...")
    for team in teams:
        print(f"    -> {team.name} ({team.abbreviation})")
        try:
            team_record = {
                # Identifiers
                "team_abbr": team.abbreviation,
                "team_name": team.name,

                # Batting Stats
                "games_played": team.games,
                "runs": team.runs,
                "hits": team.hits,
                "home_runs": team.home_runs,
                "batting_average": team.batting_average,
                "strikeouts_batting": team.strikeouts,
                "walks_batting": team.walks,

                # Pitching Stats
                "era": team.earned_run_average,
                "whip": team.walks_hits_per_inning_pitched,

                # Fielding Stats (The missing piece!)
                "errors": team.errors,

                # Metadata
                "last_updated": datetime.now().isoformat()
            }
            records.append(team_record)
        except Exception as e:
            print(f"    ⚠️ Could not process stats for team {team.abbreviation}: {e}")

    if not records:
        print("❌ No team records were successfully processed. Aborting Supabase upload.")
        return

    print(f"⬆️ Upserting {len(records)} teams' complete stats into Supabase...")
    try:
        response = supabase.table("team_stats").upsert(records, on_conflict="team_abbr").execute()
        if response.data:
            print("✅ Team stats upserted successfully.")
        else:
            # Check for a more detailed error message from the Supabase response
            error_details = getattr(response, 'error', 'Unknown error')
            print(f"❌ Supabase Error: {error_details}")
    except Exception as e:
        print(f"❌ Supabase upsert failed with an exception: {e}")

if __name__ == "__main__":
    fetch_and_upsert_team_stats()
