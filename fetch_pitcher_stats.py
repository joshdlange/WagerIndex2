import os
import uuid
import requests
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()
from dotenv import load_dotenv
load_dotenv(override=False)  # This prevents overwriting GitHub Actions env vars

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing Supabase credentials.")


supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_pitcher_stats():
    url = "https://statsapi.mlb.com/api/v1/stats"
    params = {
        "stats": "season",
        "group": "pitching",
        "season": "2024",
        "gameType": "R",
        "limit": 1000,
        "sortStat": "earnedRunAvg",
        "order": "asc"
    }
    response = requests.get(url, params=params)
    data = response.json()

    results = []
    for player in data.get("stats", [])[0].get("splits", []):
        info = player.get("player", {})
        stat = player.get("stat", {})
        mlb_id = str(info.get("id"))
        name = f"{info.get('fullName')}"
        era = float(stat.get("era", 0.0))
        whip = float(stat.get("whip", 0.0))
        k9 = float(stat.get("strikeoutsPer9Inn", 0.0))
        bb9 = float(stat.get("walksPer9Inn", 0.0))
        ip = float(stat.get("inningsPitched", 0.0))

        results.append({
            "mlb_id": mlb_id,
            "name": name,
            "era": era,
            "whip": whip,
            "k9": k9,
            "bb9": bb9,
            "innings_pitched": ip
        })

    print("📤 Previewing first 3 pitcher stats:")
    print(results[:3])
    return results

def push_to_supabase(pitchers):
    response = supabase.table("pitchers").upsert(
        pitchers, on_conflict=["mlb_id"]
    ).execute()
    print(f"✅ Supabase response: {response}")

if __name__ == "__main__":
    pitchers = fetch_pitcher_stats()
    push_to_supabase(pitchers)
