import os
import uuid
from supabase import create_client, Client
from sportsipy.mlb.teams import Teams
from dotenv import load_dotenv

# Load env vars
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_team_stats():
    results = []
    for team in Teams():
        try:
            data = team.data
            stats = {
                "id": str(uuid.uuid4()),
                "team_name": team.name,
                "batting_avg": float(data.get("batting_average", 0)),
                "slugging_pct": float(data.get("slugging_percentage", 0)),
                "on_base_pct": float(data.get("on_base_percentage", 0)),
                "runs_per_game": float(data.get("runs_per_game", 0)),
                "hits_per_game": float(data.get("hits_per_game", 0)),
                "walks_per_game": float(data.get("walks_per_game", 0)),
                "strikeouts_per_game": float(data.get("strikeouts_per_game", 0)),
                "team_era": float(data.get("earned_run_avg", 0)),
                "team_whip": float(data.get("walks_hits_per_inning_pitched", 0)),
                "fielding_pct": float(data.get("fielding_percentage", 0)),
                "errors_per_game": float(data.get("errors_per_game", 0)),
                # Placeholder values for bullpen stats
                "bullpen_era": 0.00,
                "bullpen_whip": 0.00,
            }
            results.append(stats)
        except Exception as e:
            print(f"⚠️ Error fetching stats for {team.name}: {e}")
    return results

def push_to_supabase(teams):
    try:
        supabase.table("team_stats").upsert(teams, on_conflict=["team_name"]).execute()
        print(f"✅ Uploaded {len(teams)} team stats to Supabase.")
    except Exception as e:
        print(f"❌ Supabase upload failed: {e}")

def main():
    teams = fetch_team_stats()
    if teams:
        push_to_supabase(teams)
    else:
        print("⚠️ No team stats found.")

if __name__ == "__main__":
    main()
