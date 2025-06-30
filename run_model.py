import os
import uuid
import datetime
from supabase import create_client
from dotenv import load_dotenv

from dotenv import load_dotenv
load_dotenv(override=False)  # This prevents overwriting GitHub Actions env vars

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing Supabase credentials.")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Stat weights from the spreadsheet
STAT_WEIGHTS = {
    "runs_per_game": 5,
    "hits_per_game": 5,
    "batting_avg": 4,
    "on_base_pct": 3,
    "slugging_pct": 4,
    "strikeouts_per_game": 1,
    "team_era": 5,
    "team_whip": 4,
    "bullpen_era": 3,
    "bullpen_whip": 3,
    "fielding_pct": 2,
    "errors_per_game": 1,
    "starter_era": 4,
    "starter_whip": 3,
    "starter_k9": 2,
    "starter_bb9": 2,
    "starter_hr9": 2,
}

def fetch_games_for_today():
    today = datetime.date.today().isoformat()
    return supabase.table("games").select("*").eq("game_date", today).execute().data or []

def fetch_team_stats():
    return {row["team_name"]: row for row in supabase.table("team_stats").select("*").execute().data or []}

def fetch_pitchers():
    return {row["team_id"]: row for row in supabase.table("pitchers").select("*").execute().data or []}

def winner_gets_weight(stat, val_a, val_b, weight):
    if val_a is None or val_b is None:
        return (0, 0)
    if stat in ["runs_per_game", "hits_per_game", "batting_avg", "on_base_pct", "slugging_pct",
                "fielding_pct", "starter_k9"]:
        if val_a > val_b:
            return (weight, 0)
        elif val_b > val_a:
            return (0, weight)
    else:
        if val_a < val_b:
            return (weight, 0)
        elif val_b < val_a:
            return (0, weight)
    return (0, 0)

def run_model():
    print("🔍 Running prediction model...")
    today = datetime.date.today().isoformat()

    games = fetch_games_for_today()
    teams = fetch_team_stats()
    pitchers = fetch_pitchers()
    predictions = []

    score_diffs = []
    moneyline_map = {}

    for game in games:
        home = game["home_team"]
        away = game["away_team"]
        home_stats = teams.get(home, {})
        away_stats = teams.get(away, {})

        home_pitcher = pitchers.get(game.get("home_team_id"), {})
        away_pitcher = pitchers.get(game.get("away_team_id"), {})
        for k in ["era", "whip", "k9", "bb9", "hr9"]:
            home_stats[f"starter_{k}"] = home_pitcher.get(k)
            away_stats[f"starter_{k}"] = away_pitcher.get(k)

        home_score = 0
        away_score = 0

        for stat, weight in STAT_WEIGHTS.items():
            adv_away, adv_home = winner_gets_weight(stat, away_stats.get(stat), home_stats.get(stat), weight)
            home_score += adv_home
            away_score += adv_away

        predicted_home = round(home_score / 10)
        predicted_away = round(away_score / 10)
        confidence = round(abs(home_score - away_score) / 10, 2)

        predicted_winner = home if predicted_home > predicted_away else away
        predicted_loser = away if predicted_winner == home else home
        score_diff = abs(predicted_home - predicted_away)

        moneyline = game.get("home_moneyline") if predicted_winner == home else game.get("away_moneyline")
        moneyline_map[predicted_winner] = moneyline

        predictions.append({
            "id": str(uuid.uuid4()),
            "prediction_date": today,
            "home_team": home,
            "away_team": away,
            "predicted_winner": predicted_winner,
            "predicted_home_score": predicted_home,
            "predicted_away_score": predicted_away,
            "confidence": confidence,
            "model_version": "v2.0",
            "pick_of_the_day": False  # default
        })

        score_diffs.append({
            "team": predicted_winner,
            "score_diff": score_diff,
            "moneyline": moneyline
        })

    # Rank by score_diff and moneyline
    score_diffs.sort(key=lambda x: -x["score_diff"])
    for idx, item in enumerate(score_diffs):
        item["score_rank"] = idx + 1

    score_diffs.sort(key=lambda x: -x["moneyline"] if x["moneyline"] is not None else -9999)
    for idx, item in enumerate(score_diffs):
        item["moneyline_rank"] = idx + 1
        item["combined_rank"] = (item["score_rank"] + item["moneyline_rank"]) / 2

    # Pick of the day = lowest combined rank
    pick = sorted(score_diffs, key=lambda x: x["combined_rank"])[0]["team"]

    for prediction in predictions:
        if prediction["predicted_winner"] == pick:
            prediction["pick_of_the_day"] = True

    print(f"⭐ Pick of the Day: {pick}")
    supabase.table("predictions").upsert(predictions, on_conflict=["prediction_date", "home_team", "away_team"]).execute()
    print("✅ Predictions saved.")

if __name__ == "__main__":
    run_model()
