# fetch_team_stats.py
import os
from supabase import create_client, Client
from sportsipy.mlb.teams import Teams
from dotenv import load_dotenv
from datetime import datetime
import sys

load_dotenv()

# --- Config ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
CURRENT_YEAR = datetime.now().year

def fetch_and_upsert_team_stats():
    """
    Fetches team stats for the CURRENT season ONLY. If data is not available,
    it logs a message and exits gracefully.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Fatal Error: SUPABASE_URL and SUPABASE_KEY secrets must be set.")
        sys.exit(1) # Exit with an error code

    print(f"📊 Fetching team stats for the {CURRENT_YEAR} season using sportsipy...")

    try:
        teams = Teams(CURRENT_YEAR)
        # The Teams object can be empty if the season hasn't started.
        # We check its length to confirm if data was returned.
        if not teams or len(list(teams)) == 0:
            print(f"✅ INFO: No team stats data found for {CURRENT_YEAR}. This is expected before the season starts. Stopping job.")
            sys.exit(0) # Exit gracefully
    except Exception as e:
        print(f"❌ Error fetching data from sportsipy for {CURRENT_YEAR}: {e}")
        sys.exit(1)

    records = []
    print("  - Processing each team...")
    for team in teams:
        try:
            records.append({
                "team_abbr": team.abbreviation, "team_name": team.name,
                "games_played": team.games, "runs": team.runs, "hits": team.hits,
                "home_runs": team.home_runs, "batting_average": team.batting_average,
                "strikeouts_batting": team.strikeouts, "walks_batting": team.walks,
                "era": team.earned_run_average, "whip": team.walks_hits_per_inning_pitched,
                "errors": team.errors, "last_updated": datetime.now().isoformat()
            })
        except Exception:
            # Skip any team that might have incomplete data, but log it
            print(f"    ⚠️ Could not fully process stats for team {team.abbreviation}. Skipping.")

    if not records:
        print("✅ INFO: No team records were successfully processed. Stopping job.")
        sys.exit(0)

    print(f"⬆️ Upserting {len(records)} teams' complete stats into Supabase...")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        response = supabase.table("team_stats").upsert(records, on_conflict="team_abbr").execute()
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
