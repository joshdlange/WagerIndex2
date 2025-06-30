import os
import requests
import uuid
from supabase import create_client
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_pitchers_stats(year=2024):
    print(f"🔄 Fetching pitcher data for season {year}")
    url = f"https://statsapi.mlb.com/api/v1/stats?stats=season&group=pitching&limit=1000&season={year}&sportIds=1"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"❌ Error fetching pitcher stats: {e}")
        return

    stats = data.get("stats", [])
    if not stats or not stats[0].get("splits"):
        print("❌ No pitcher stats found.")
        return

    for player in stats[0]["splits"]:
        person = player.get("player", {})
        stat = player.get("stat", {})
        mlb_id = person.get("id")
        name = person.get("fullName", "Unknown")

        pitcher_record = {
            "id": str(uuid.uuid4()),
            "mlb_id": str(mlb_id),
            "name": name,
            "team_id": None,  # Will update this if team mapping is handled elsewhere
            "era": float(stat.get("era", 0.0)),
            "whip": float(stat.get("whip", 0.0)),
            "k9": float(stat.get("strikeoutsPer9Inn", 0.0)),
            "bb9": float(stat.get("baseOnBallsPer9", 0.0)),
            "innings_pitched": float(stat.get("inningsPitched", 0.0)),
        }

        # UPSERT using mlb_id as unique key if supported
        try:
            supabase.table("pitchers").upsert(pitcher_record, on_conflict="mlb_id").execute()
        except Exception as e:
            print(f"❌ Error inserting pitcher {name}: {e}")

    print(f"✅ Finished syncing pitcher stats.")

if __name__ == "__main__":
    fetch_pitchers_stats()
