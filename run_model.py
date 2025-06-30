import os
import uuid
import datetime
from supabase import create_client
from dotenv import load_dotenv

# Load .env for local testing
load_dotenv()

# Supabase config
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Stat weights directly from spreadsheet
STAT_WEIGHTS = {
    "batting_avg": 4,
    "slugging_pct": 4,
    "on_base_pct": 3,
    "runs_per_game": 5,
    "hits_per_game": 2,
    "walks_per_game": 2,
    "strikeouts_per_game": 1,
    "team_era": 5,
    "team_whip": 4,
    "starter_era": 5,
    "starter_whip": 4,
    "bullpen_era": 3,
    "bullpen_whip": 3,
    "fielding_pct": 2,
    "errors_per_game": 1,
    "blowout_last_game": 4,
    "team_streak": 1,
}

def fetch_games_for_today():
    today = datetime.date.today().isoformat()
    return supabase.table("games").select("*").eq("game_date", today).execute().data or []

def fetch_team_stats():
    return {row["team_name"]: row for row in supabase.table("team_stats").select("*").execute().data or []}

def compute_score(team_stats, weights):
    return sum((team_stats.get(stat, 0) * weight) for stat, weight in weights.items())

def run_model():
    print("📥 Running model...")

    games = fetch_games_for_today()
    team_stats = fetch_team_stats()
    predictions = []
    today = datetime.date.today().isoformat()

    for game in games:
        home = game["home_team"]
        away = game["away_team"]

        home_score = compute_score(team_stats.get(home, {}), STAT_WEIGHTS)
        away_score = compute_score(team_stats.get(away, {}), STAT_WEIGHTS)

        predicted_home = round(home_score / 10)
        predicted_away = round(away_score / 10)

        predicted_winner = home if predicted_home > predicted_away else away
        confidence = round(abs(home_score - away_score) / 100, 2)

        predictions.append({
            "id": str(uuid.uuid4()),
            "prediction_date": today,
            "home_team": home,
            "away_team": away,
            "predicted_home_score": predicted_home,
            "predicted_away_score": predicted_away,
            "predicted_winner": predicted_winner,
            "confidence": confidence,
            "model_version": "v1.0"
        })

    print("📤 Previewing first 3 predictions:")
    print(predictions[:3])

    supabase.table("predictions").upsert(predictions, on_conflict=["prediction_date", "home_team", "away_team"]).execute()
    print("✅ Predictions saved.")

if __name__ == "__main__":
    run_model()
