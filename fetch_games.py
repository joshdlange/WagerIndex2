import os
import requests
from datetime import date
from supabase import create_client

# --- Supabase setup ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- ESPN endpoint for today’s games ---
def fetch_espn_games():
    today = date.today().isoformat()
    url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={today}"
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"❌ Failed to fetch games: {response.status_code}")
        return []

    data = response.json()
    events = data.get("events", [])
    games = []

    for event in events:
        competition = event.get("competitions", [{}])[0]
        competitors = competition.get("competitors", [])

        home_team = next((c["team"]["abbreviation"] for c in competitors if c["homeAway"] == "home"), None)
        away_team = next((c["team"]["abbreviation"] for c in competitors if c["homeAway"] == "away"), None)

        game = {
            "home_team": home_team,
            "away_team": away_team,
            "game_date": date.today().isoformat()
        }

        games.append(game)

    return games

# --- Insert into Supabase ---
def insert_games_into_db(games):
    if not games:
        print("⚠️ No games to insert.")
        return

    for game in games:
        print(f"Inserting: {game}")
        try:
            supabase.table("games").insert(game).execute()
        except Exception as e:
            print(f"❌ Insert failed: {e}")

# --- Run ---
if __name__ == "__main__":
    games = fetch_espn_games()
    insert_games_into_db(games)
