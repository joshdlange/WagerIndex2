import os
from datetime import datetime, timedelta
import requests
from supabase import create_client
import uuid

# Supabase config (GitHub Actions uses secrets)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_espn_game_results(date_str):
    url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={date_str}"
    response = requests.get(url)
    data = response.json()

    results = []
    for event in data.get("events", []):
        try:
            game_id = event["id"]
            competitions = event["competitions"][0]
            competitors = competitions["competitors"]
            home = next(team for team in competitors if team["homeAway"] == "home")
            away = next(team for team in competitors if team["homeAway"] == "away")

            home_score = int(home["score"])
            away_score = int(away["score"])
            winner = home["team"]["abbreviation"] if home_score > away_score else away["team"]["abbreviation"]

            result = {
                "id": str(uuid.uuid4()),
                "espn_game_id": game_id,
                "game_date": date_str,
                "home_score": home_score,
                "away_score": away_score,
                "winner": winner
            }
            results.append(result)
        except Exception as e:
            print(f"⚠️ Skipped game due to error: {e}")

    return results

def push_to_supabase(results):
    if not results:
        print("⚠️ No game results to insert.")
        return
    response = supabase.table("games").upsert(results, on_conflict=["espn_game_id"]).execute()
    print("✅ Supabase response:", response)

if __name__ == "__main__":
    date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"📅 Fetching game results for {date_str}")
    results = fetch_espn_game_results(date_str)
    print("📤 Previewing first 3 results:", results[:3])
    push_to_supabase(results)
