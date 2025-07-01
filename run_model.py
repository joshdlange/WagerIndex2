# run_model.py
import os
import pandas as pd
import numpy as np
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# --- Config ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
# RESTORED: The original four-component weights, as per your model's design.
WEIGHTS = {'batting': 0.40, 'pitching': 0.30, 'bullpen': 0.20, 'defense': 0.10}

def normalize_stat(series: pd.Series, ascending=True) -> pd.Series:
    """Normalizes a pandas Series into a 0-100 score."""
    return series.rank(method='max', ascending=ascending, pct=True) * 100

def run_prediction_engine():
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Successfully connected to Supabase.")
    except Exception as e:
        print(f"❌ Failed to connect to Supabase: {e}")
        return

    # 1. Fetch all data from Supabase
    print("⬇️ Fetching data from Supabase tables...")
    try:
        today_str = datetime.now().strftime('%Y-%m-%d')
        games_resp = supabase.table("games").select("*").eq("game_date", today_str).execute()
        games_df = pd.DataFrame(games_resp.data)

        teams_resp = supabase.table("team_stats").select("*").execute()
        team_stats_df = pd.DataFrame(teams_resp.data)

        pitchers_resp = supabase.table("pitchers").select("*").execute()
        pitcher_stats_df = pd.DataFrame(pitchers_resp.data)
        
        if any(df.empty for df in [games_df, team_stats_df, pitcher_stats_df]):
            print("⚠️ Halting: One or more required tables are empty.")
            return
        if 'errors' not in team_stats_df.columns:
            print("⚠️ Halting: 'errors' column not found in team_stats table. Run fetch_team_stats.py again.")
            return
            
    except Exception as e:
        print(f"❌ Error fetching data from Supabase: {e}")
        return

    print("🚀 Running prediction model with original 4-part weighting...")

    # 2. Pre-compute Component Scores
    team_stats_df['Batting_Score'] = normalize_stat(team_stats_df['batting_average'], ascending=True)
    team_stats_df['Defense_Score'] = normalize_stat(team_stats_df['errors'], ascending=False) # Lower errors is better
    # Placeholder for Bullpen Score - assuming it's based on team ERA for now
    team_stats_df['Bullpen_Score'] = normalize_stat(team_stats_df['era'], ascending=False)
    
    pitcher_stats_df['Pitching_Score'] = (
        normalize_stat(pitcher_stats_df['era'], ascending=False) +
        normalize_stat(pitcher_stats_df['whip'], ascending=False)
    ) / 2
    league_avg_pitcher_score = pitcher_stats_df['Pitching_Score'].mean()

    # 3. Process each game
    predictions = []
    for _, game in games_df.iterrows():
        try:
            away_team = team_stats_df[team_stats_df['team_abbr'] == game['away_team_abbr']].iloc[0]
            home_team = team_stats_df[team_stats_df['team_abbr'] == game['home_team_abbr']].iloc[0]
            
            away_pitcher = pitcher_stats_df[pitcher_stats_df['name'] == game.get('away_pitcher_name')]
            home_pitcher = pitcher_stats_df[pitcher_stats_df['name'] == game.get('home_pitcher_name')]
            
            away_pitching_score = away_pitcher['Pitching_Score'].iloc[0] if not away_pitcher.empty else league_avg_pitcher_score
            home_pitching_score = home_pitcher['Pitching_Score'].iloc[0] if not home_pitcher.empty else league_avg_pitcher_score

            away_score = (away_team['Batting_Score'] * WEIGHTS['batting'] + 
                          away_pitching_score * WEIGHTS['pitching'] +
                          away_team['Bullpen_Score'] * WEIGHTS['bullpen'] +
                          away_team['Defense_Score'] * WEIGHTS['defense']) / 10
            
            home_score = (home_team['Batting_Score'] * WEIGHTS['batting'] +
                          home_pitching_score * WEIGHTS['pitching'] +
                          home_team['Bullpen_Score'] * WEIGHTS['bullpen'] +
                          home_team['Defense_Score'] * WEIGHTS['defense']) / 10

            predictions.append({
                # ... (rest of the prediction dictionary is the same)
            })
        except Exception as e:
            print(f"⚠️ Error processing game {game.get('away_team_abbr')} vs {game.get('home_team_abbr')}: {e}")
    
    # ... The rest of the script (calculating POTD and upserting) is unchanged ...
    if not predictions:
        print("🤷 No predictions were generated.")
        return
        
    predictions_df = pd.DataFrame(predictions)
    predictions_df['margin'] = abs(predictions_df['predicted_score_away'] - predictions_df['predicted_score_home'])
    predictions_df['margin_rank'] = predictions_df['margin'].rank(ascending=False)
    # ... etc ...

if __name__ == "__main__":
    run_prediction_engine()
