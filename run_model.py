import os
import datetime
from supabase import create_client
from dotenv import load_dotenv

# Load environment variables (for GitHub Actions)
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def run_model():
    print("🧠 Running prediction model...")

    # 1. Get today's games
    today = datetime.datetime.now().date().isoformat()
    games_resp = supabase.table("games").select("*").eq("game_date", today).execute()
    games = games_resp.data or []
    if not games:
        print("⚠️ No games found for today.")
        return

    # 2. Get pitcher stats
    pitcher_resp = supabase.table("pitchers").select("*").execute()
    pitcher_stats = {p["id"]: p for p in pitcher_resp.data}

    # 3. Get team stats
    team_resp = supabase.table("team_stats").select("*").execute()
    team_stats = {t["team_id"]: t for t in team_resp.data}

    score_diffs = []

    for game in games:
        home_id = game.get("home_team_id")
        away_id = game.get("away_team_id")
        home_pitcher_id = game.get("home_pitcher_id")
        away_pitcher_id = game.get("away_pitcher_id")

        # Skip if any core data is missing
        if not all([home_id, away_id, home_pitcher_id, away_pitcher_id]):
            continue

        home_pitcher = pitcher_stats.get(home_pitcher_id)
        away_pitcher = pitcher_stats.get(away_pitcher_id)
        home_team = team_stats.get(home_id)
        away_team = team_stats.get(away_id)

        if not all([home_pitcher, away_pitcher, home_team, away_team]):
            continue

        # 4. Example scoring logic
        home_score = (
            (5 - home_pitcher["era"])
            + (5 - home_pitcher["whip"])
            + home_team["runs"]
            + home_team["ops"]
        )
        away_score = (
            (5 - away_pitcher["era"])
            + (5 - away_pitcher["whip"])
            + away_team["runs"]
            + away_team["ops"]
        )

        score_diffs.append({
            "game_id": game["id"],
            "home_team": game["home_team"],
            "away_team": game["away_team"],
            "home_score": round(home_score, 2),
            "away_score": round(away_score, 2),
            "combined_rank": abs(home_score - away_score),  # lower is closer matchup
            "team": game["home_team"] if home_score > away_score else game["away_team"]
        })

    print(f"🔍 Matchups with computed scores: {len(score_diffs)}")

    if not score_diffs:
        print("❌ No matchups available to score — skipping pick generation.")
        return

    pick = sorted(score_diffs, key=lambda x: x["combined_rank"])[0]["team"]
    print(f"✅ Model Pick of the Day: {pick}")

    # Optionally upsert this to a new table
    # supabase.table("model_picks").upsert({"date": today, "team": pick}).execute()

if __name__ == "__main__":
    run_model()
