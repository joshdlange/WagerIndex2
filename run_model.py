import os
from supabase import create_client
from datetime import date
import pandas as pd

# 🔐 HARD-CODED SUPABASE CREDS (update before running)
SUPABASE_URL = "https://ychlgelqelznjhevnxpc.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InljaGxnZWxxZWx6bmpoZXZueHBjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTA4NzU2MzQsImV4cCI6MjA2NjQ1MTYzNH0.-O5EzjscmfLW3nfohvy1CAr5f9wFG-a6sVUcGLKOHlw"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ⚙️ MODEL WEIGHTS (adjust if needed)
WEIGHTS = {
    "runs_per_game": 0.4,
    "era": -0.3,
    "whip": -0.2,
    "k9": 0.1,
    "bb9": -0.1
}

def fetch_games_for_today():
    today_str = str(date.today())
    print(f"📅 Fetching games for {today_str}")
    response = supabase.table("games").select("*").eq("game_date", today_str).execute()
    return response.data if response.data else []

def fetch_team_stats():
    print("📊 Fetching team stats")
    response = supabase.table("team_stats").select("*").execute()
    df = pd.DataFrame(response.data)
    return df.set_index("team_abbr") if not df.empty else pd.DataFrame()

def run_model(games, stats_df):
    print("🧠 Running prediction model...")
    results = []
    for game in games:
        away = game["away_team"]
        home = game["home_team"]

        if away not in stats_df.index or home not in stats_df.index:
            print(f"⚠️ Skipping {away} vs {home}: missing team stats")
            continue

        away_stats = stats_df.loc[away]
        home_stats = stats_df.loc[home]

        def score(stats):
            return sum(stats.get(stat, 0) * weight for stat, weight in WEIGHTS.items())

        away_score = score(away_stats)
        home_score = score(home_stats)

        predicted_winner = away if away_score > home_score else home
        score_diff = abs(round(home_score - away_score, 2))

        results.append({
            "game_id": game["id"],
            "predicted_winner": predicted_winner,
            "score_diff": score_diff,
        })

        print(f"🏟️ {away} vs {home} → 🧮 {predicted_winner} by {score_diff}")

    return results

def push_predictions(results):
    if not results:
        print("⚠️ No results to push")
        return

    print("🚀 Uploading predictions to Supabase...")
    for result in results:
        print(f"🔁 {result}")
        supabase.table("game_results").upsert(result).execute()

if __name__ == "__main__":
    games = fetch_games_for_today()
    stats_df = fetch_team_stats()
    results = run_model(games, stats_df)
    push_predictions(results)
