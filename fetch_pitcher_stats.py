import os
import uuid
from supabase import create_client, Client
from sportsipy.mlb.teams import Teams
from dotenv import load_dotenv

# Load env vars
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_pitchers():
    all_pitchers = []
    teams = Teams()
    for team in teams:
        try:
            for player in team.roster.players:
                p = player.data
                if p["position"] != "P":
                    continue

                pitcher = {
                    "id": str(uuid.uuid4()),
                    "name": p.get("name"),
                    "era": float(p.get("earned_run_avg", 0)),
                    "whip": float(p.get("walks_hits_per_inning_pitched", 0)),
                    "k9": float(p.get("strikeouts_per_nine", 0)),
                    "bb9": float(p.get("walks_per_nine", 0)),
                    "innings_pitched": float(p.get("innings_pitched", 0)),
                }
                all_pitchers.append(pitcher)
        except Exception as e:
            print(f"⚠️ Error processing team {team.name}: {e}")
    return all_pitchers

def push_to_supabase(pitchers):
    try:
        supabase.table("pitchers").upsert(pitchers, on_conflict=["name"]).execute()
        print(f"✅ Uploaded {len(pitchers)} pitchers to Supabase.")
    except Exception as e:
        print(f"❌ Supabase upload failed: {e}")

def main():
    pitchers = fetch_pitchers()
    if pitchers:
        push_to_supabase(pitchers)
    else:
        print("⚠️ No pitcher stats found.")

if __name__ == "__main__":
    main()
