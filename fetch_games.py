import os
import uuid
import datetime
import requests
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_espn_games(target_date):
    date_str = target_date.strftime("%Y%m%d")
    url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={date_str}"
    response = requests.get(url)
    data = response.json()

    games = []
    for event in data.get("events", []):
        competition = event["competitions"][0]
        competitors = competition["competitors"]
        if len(competitors) != 2:
            continue

        home = next(c for c in competitors if c["homeAway"] == "home")
        away = next(c for c in competitors if c["homeAway"] == "away")

        game = {
            "id": str(uuid.uuid4()),
            "game_date": target_date.isoformat(),
            "home_team": home["team"]["displayName"],
            "home_team_abbr": home["team"]["abbreviation"],
            "away_team": away["team"]["displayName"],
            "away_team_abbr": away["team"]["abbreviation"],
            "espn_game_id": event["id"]
        }
        games.append(game)
    return games

def push_to_supabase(games):
    if not games:
        return
    supabase.table("games").upsert(games).execute()

if __name__ == "__main__":
    today = datetime.date.today()
    games = fetch_espn_games(today)
    push_to_supabase(games)
