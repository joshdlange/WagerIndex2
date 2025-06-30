
import os
import requests
from supabase import create_client
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Supabase setup
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_today_games():
    print("🎯 Fetching today's games from ESPN...")
    today = datetime.today().strftime('%Y%m%d')
    url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={today}"
    resp = requests.get(url)
    data = resp.json()
    events = data.get("events", [])
    games = []

    for event in events:
        competitions = event.get("competitions", [])
        if not competitions:
            continue
        comp = competitions[0]
        competitors = comp.get("competitors", [])
        if len(competitors) != 2:
            continue

        home = next((team for team in competitors if team["homeAway"] == "home"), None)
        away = next((team for team in competitors if team["homeAway"] == "away"), None)

        if not home or not away:
            continue

        game = {
            "game_date": datetime.fromisoformat(event["date"]).date().isoformat(),
            "home_team": home["team"]["displayName"],
            "away_team": away["team"]["displayName"],
            "home_team_abbr": home["team"]["abbreviation"],
            "away_team_abbr": away["team"]["abbreviation"],
            "espn_game_id": event["id"],
            "home_score": int(home.get("score", 0)),
            "away_score": int(away.get("score", 0)),
        }

        # Dummy predicted winner and confidence for now
        game["predicted_winner"] = game["home_team"] if game["home_score"] > game["away_score"] else game["away_team"]
        game["score_home"] = game["home_score"]
        game["score_away"] = game["away_score"]
        game["confidence_score"] = 0.75  # Placeholder confidence
        games.append(game)

    return games

def run_model():
    print("🚀 Running prediction model...")
    games = fetch_today_games()
    if not games:
        print("⚠️ No games found for today.")
        return

    # Identify pick of the day (highest confidence)
    pick_of_the_day = max(games, key=lambda x: x["confidence_score"])
    for game in games:
        game["is_pick_of_the_day"] = game == pick_of_the_day

    # Store in Supabase
    for game in games:
        pick_data = {
            "game_date": game["game_date"],
            "away_team": game["away_team"],
            "home_team": game["home_team"],
            "predicted_winner": game["predicted_winner"],
            "score_away": game["score_away"],
            "score_home": game["score_home"],
            "confidence_score": game["confidence_score"],
            "is_pick_of_the_day": game.get("is_pick_of_the_day", False)
        }

        try:
            supabase.table("daily_picks").upsert(pick_data).execute()
            print(f"✅ Stored pick: {pick_data['predicted_winner']} in {pick_data['home_team']} vs {pick_data['away_team']}")
        except Exception as e:
            print(f"❌ Failed to insert pick: {e}")

if __name__ == "__main__":
    run_model()
