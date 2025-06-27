import uuid
from datetime import date
from supabase import create_client, Client

# === GitHub-ready config ===
import os
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def calculate_score(team_stats):
    return (
        team_stats.get("batting_avg", 0) * 100 +
        team_stats.get("ops", 0) * 50 -
        team_stats.get("era", 0) * 10 +
        team_stats.get("home_runs", 0) * 0.1
    )

def predict_game(game, team_stats_lookup):
    home = game["home_team"]
    away = game["away_team"]
    home_stats = team_stats_lookup.get(home)
    away_stats = team_stats_lookup.get(away)

    if not home_stats or not away_stats:
        return None

    home_score = calculate_score(home_stats)
    away_score = calculate_score(away_stats)

    predicted_winner = home if home_score > away_score else away
    return {
        "id": str(uuid.uuid4()),
        "espn_game_id": game["espn_game_id"],
        "game_date": game["game_date"],
        "predicted_home_score": round(home_score, 1),
        "predicted_away_score": round(away_score, 1),
        "predicted_winner": predicted_winner,
    }

# === Fetch today's games ===
games_resp = supabase.table("games").select("*").eq("game_date", str(date.today())).execute()
games = games_resp.data

# === Fetch all team stats ===
team_stats_resp = supabase.table("team_stats").select("*").execute()
team_stats = team_stats_resp.data
team_lookup = {team["team_abbr"]: team for team in team_stats}

# === Predict outcomes ===
predictions = []
for game in games:
    prediction = predict_game(game, team_lookup)
    if prediction:
        predictions.append(prediction)

print(f"📊 Generated {len(predictions)} predictions. Preview:")
print(predictions[:3])

# === Upsert predictions into Supabase ===
if predictions:
    supabase.table("game_predictions").upsert(predictions, on_conflict=["espn_game_id"]).execute()
    print("✅ Predictions uploaded.")
else:
    print("⚠️ No predictions generated.")
