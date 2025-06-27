import requests
from supabase import create_client
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_daily_games():
    today = datetime.today().strftime("%Y%m%d")
    url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={today}"

    print(f"🔁 Fetching games for {today}")
    resp = requests.get(url)

    if resp.status_code != 200:
        print(f"❌ Failed to fetch games: {resp.status_code}")
        return []

    events = resp.json().get("events", [])
    games = []

    for event in events:
        try:
            game_id = event["id"]
            competitions = event["competitions"][0]
            competitors = competitions["competitors"]

            home = next(team for team in competitors if team["homeAway"] == "home")
            away = next(team for team in competitors if team["homeAway"] == "away")

            game = {
                "espn_game_id": game_id,
                "date": event["date"],
                "home_team_abbr": home["team"]["abbreviation"],
                "away_team_abbr": away["team"]["abbreviation"],
                "home_score": int(home["score"]),
                "away_score": int(away["score"]),
                "status": event["status"]["type"]["name"]
            }
            games.append(game)
        except Exception as e:
            print(f"⚠️ Error parsing game: {e}")
            continue

    return games

def upsert_games(games):
    for g in games:
        print(f"📤 Inserting game {g['away_team_abbr']} vs {g['home_team_abbr']}...")
        try:
            supabase.table("games").upsert(g).execute()
        except Exception as e:
            print(f"❌ Failed to upsert game: {e}")

if __name__ == "__main__":
    games = fetch_daily_games()
    if games:
        upsert_games(games)
    else:
        print("⚠️ No games found.")
