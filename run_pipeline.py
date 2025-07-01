# run_pipeline.py
import os
import sys
import requests
import pandas as pd
import numpy as np
import io
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime
from utils import get_current_season_year
from pybaseball import pitching_stats as pybaseball_pitching_stats

# --- CONFIGURATION ---
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
WEIGHTS = {'batting': 0.40, 'pitching': 0.30, 'bullpen': 0.20, 'defense': 0.10}

# --- DATABASE INTERACTION ---
def get_supabase_client():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Fatal Error: SUPABASE_URL and SUPABASE_KEY secrets must be set.")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def upsert_data(supabase_client, table_name, records, conflict_column):
    if not records:
        print(f"✅ No new records to upsert for table '{table_name}'.")
        return
    print(f"⬆️ Upserting {len(records)} records to '{table_name}' table...")
    try:
        response = supabase_client.table(table_name).upsert(records, on_conflict=conflict_column).execute()
        if response.data:
            print(f"✅ Successfully upserted data to '{table_name}'.")
        else:
            print(f"❌ Supabase Error for '{table_name}': {getattr(response, 'error', 'Unknown error')}")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Supabase upsert for '{table_name}' failed with an exception: {e}")
        sys.exit(1)

# --- DATA FETCHING ---
def fetch_team_stats(year):
    print(f"\n--- 1. Fetching Team Stats for {year} ---")
    
    # This dictionary will hold all stats, keyed by team name. This is safer than merging.
    team_stats_map = {}

    # Define URLs and the stats to extract from each
    urls_and_stats = {
        'batting': (f"https://www.espn.com/mlb/stats/team/_/season/{year}", {'GP':'games_played', 'R':'runs', 'H':'hits', 'HR':'home_runs', 'AVG':'batting_avg', 'OBP':'obp', 'SLG':'slugging'}),
        'pitching': (f"https://www.espn.com/mlb/stats/team/_/view/pitching/season/{year}", {'ERA':'era', 'WHIP':'whip', 'SO':'strikeouts_per_9', 'BB':'walks_per_9', 'SV':'save_pct'}),
        'fielding': (f"https://www.espn.com/mlb/stats/team/_/view/fielding/season/{year}", {'FPCT':'fielding_pct', 'E':'errors_per_game'}) # Note: ESPN 'E' is total errors
    }

    for category, (url, stat_mapping) in urls_and_stats.items():
        print(f"  -> Scraping {category} data...")
        try:
            response = requests.get(url, headers=HEADERS, timeout=15)
            response.raise_for_status()
            tables = pd.read_html(io.StringIO(response.text))
            df = pd.concat([tables[0], tables[1]], ignore_index=True)
            
            # Clean dataframe
            first_col_name = df.columns[0]
            df = df[df[first_col_name] != first_col_name]
            df = df.rename(columns={first_col_name: 'TeamNameRaw'})
            df['TeamName'] = df['TeamNameRaw'].astype(str).str.replace(r'[A-Z]{2,3}$', '', regex=True).str.strip()

            # Populate the master dictionary
            for _, row in df.iterrows():
                team_name = row['TeamName']
                if team_name not in team_stats_map:
                    team_stats_map[team_name] = {'team_name': team_name} # Initialize
                
                for espn_col, supabase_col in stat_mapping.items():
                    if espn_col in row:
                        team_stats_map[team_name][supabase_col] = row[espn_col]
        
        except Exception as e:
            print(f"❌ Fatal Error scraping {category} stats: {e}")
            sys.exit(1)

    # Convert map to list of records and add abbreviation
    team_data_url = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams"
    teams_json = requests.get(team_data_url, headers=HEADERS).json()['sports'][0]['leagues'][0]['teams']
    team_abbr_map = {team['team']['displayName']: team['team']['abbreviation'] for team in teams_json}
    
    final_records = list(team_stats_map.values())
    for record in final_records:
        record['team_abbr'] = team_abbr_map.get(record['team_name'])

    df = pd.DataFrame(final_records).dropna(subset=['team_abbr'])
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    return df.where(pd.notnull(df), None).to_dict('records')

def fetch_pitcher_stats(year):
    print(f"\n--- 2. Fetching Pitcher Stats for {year} ---")
    try:
        pitchers_df = pybaseball_pitching_stats(year)
        if pitchers_df.empty:
            print(f"❌ Fatal Error: No pitcher stats found for {year}. Aborting.")
            sys.exit(1)

        filtered_df = pitchers_df[pitchers_df['IP'] >= 10].copy()
        filtered_df.replace([np.inf, -np.inf], np.nan, inplace=True)
        
        # Map to your schema exactly
        final_records = []
        for _, row in filtered_df.where(pd.notnull(filtered_df), None).iterrows():
            final_records.append({
                'name': row.get('Name'),
                'team_abbr': row.get('Team'), # We'll link via team_abbr, not team_id for simplicity
                'era': row.get('ERA'),
                'whip': row.get('WHIP'),
                'k9': row.get('K/9'),
                'bb9': row.get('BB/9'),
                'innings_pitched': row.get('IP')
            })
        return final_records
    except Exception as e:
        print(f"❌ Fatal Error fetching pitcher stats: {e}")
        sys.exit(1)

def fetch_daily_games():
    print("\n--- 3. Fetching Today's Games ---")
    try:
        url = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard"
        response = requests.get(url, headers=HEADERS).json()
        
        games_records = []
        for event in response.get("events", []):
            comp = event["competitions"][0]
            home_data = next((c for c in comp["competitors"] if c["homeAway"] == "home"), {})
            away_data = next((c for c in comp["competitors"] if c["homeAway"] == "away"), {})

            record = {
                'game_date': event.get("date", "").split("T")[0],
                'espn_game_id': event.get("id"),
                'home_team_abbr': home_data.get("team", {}).get("abbreviation"),
                'away_team_abbr': away_data.get("team", {}).get("abbreviation"),
                'home_pitcher': home_data.get("probablePitcher", {}).get("athlete", {}).get("displayName"),
                'away_pitcher': away_data.get("probablePitcher", {}).get("athlete", {}).get("displayName"),
            }
            # Safely get moneyline
            try:
                odds = next(o for o in comp.get('odds', []) if 'moneyLine' in o)
                record['home_moneyline'] = odds.get('homeTeamOdds', {}).get('moneyLine')
                record['away_moneyline'] = odds.get('awayTeamOdds', {}).get('moneyLine')
            except StopIteration:
                record['home_moneyline'], record['away_moneyline'] = None, None
            
            games_records.append(record)
        return games_records
    except Exception as e:
        print(f"❌ Fatal Error fetching daily games: {e}")
        sys.exit(1)

# --- MODEL LOGIC ---
def normalize_stat(series, ascending=True):
    return series.rank(method='max', ascending=ascending, pct=True) * 100

def run_prediction_model(games, team_stats_df, pitcher_stats_df):
    print("\n--- 4. Running Prediction Model ---")
    if games.empty or team_stats_df.empty or pitcher_stats_df.empty:
        print("⚠️ Halting model: One or more required dataframes is empty.")
        return []

    # Calculate component scores
    team_stats_df['batting_score'] = normalize_stat(team_stats_df['batting_avg'], ascending=True)
    team_stats_df['defense_score'] = normalize_stat(team_stats_df['fielding_pct'], ascending=True)
    team_stats_df['bullpen_score'] = normalize_stat(team_stats_df['era'], ascending=False)
    pitcher_stats_df['pitching_score'] = (normalize_stat(pitcher_stats_df['era'], ascending=False) + normalize_stat(pitcher_stats_df['whip'], ascending=False)) / 2
    league_avg_pitcher_score = pitcher_stats_df['pitching_score'].mean()

    predictions = []
    for _, game in games.iterrows():
        try:
            away_team = team_stats_df[team_stats_df['team_abbr'] == game['away_team_abbr']].iloc[0]
            home_team = team_stats_df[team_stats_df['team_abbr'] == game['home_team_abbr']].iloc[0]
            away_pitcher = pitcher_stats_df[pitcher_stats_df['name'] == game['away_pitcher']]
            home_pitcher = pitcher_stats_df[pitcher_stats_df['name'] == game['home_pitcher']]
            
            away_pitching_score = away_pitcher['pitching_score'].iloc[0] if not away_pitcher.empty else league_avg_pitcher_score
            home_pitching_score = home_pitcher['pitching_score'].iloc[0] if not home_pitcher.empty else league_avg_pitcher_score

            away_score = (away_team['batting_score'] * WEIGHTS['batting'] + away_pitching_score * WEIGHTS['pitching'] + away_team['bullpen_score'] * WEIGHTS['bullpen'] + away_team['defense_score'] * WEIGHTS['defense'])
            home_score = (home_team['batting_score'] * WEIGHTS['batting'] + home_pitching_score * WEIGHTS['pitching'] + home_team['bullpen_score'] * WEIGHTS['bullpen'] + home_team['defense_score'] * WEIGHTS['defense'])

            # Add prediction record mapping to 'daily_picks' schema
            predictions.append({
                'pick_date': game['game_date'],
                'home_team': game['home_team_abbr'],
                'away_team': game['away_team_abbr'],
                'predicted_winner': game['home_team_abbr'] if home_score > away_score else game['away_team_abbr'],
                'confidence_score': abs(home_score - away_score) # Margin as confidence
            })
        except Exception as e:
            print(f"⚠️ Error processing game {game['away_team_abbr']} vs {game['home_team_abbr']}: {e}")
    
    # Determine Pick of the Day
    if predictions:
        max_confidence_pick = max(predictions, key=lambda x: x['confidence_score'])
        for pick in predictions:
            pick['is_pick_of_day'] = (pick == max_confidence_pick)
    
    return predictions

# --- MAIN WORKFLOW ---
def main():
    print("🚀 Starting WagerIndex Daily Pipeline...")
    supabase = get_supabase_client()
    
    try:
        season_year = get_current_season_year()
    except Exception:
        sys.exit(1) # Exit if year fetch fails

    team_stats = fetch_team_stats(season_year)
    pitcher_stats = fetch_pitcher_stats(season_year)
    games = fetch_daily_games()
    
    # Upsert all fetched data
    upsert_data(supabase, 'team_stats', team_stats, 'team_abbr')
    # For pitchers, we'll use 'name' as the unique identifier
    upsert_data(supabase, 'pitchers', pitcher_stats, 'name')
    # For games, espn_game_id is the best unique key
    upsert_data(supabase, 'games', games, 'espn_game_id')
    
    # Convert lists of dicts to DataFrames for the model
    team_stats_df = pd.DataFrame(team_stats)
    pitcher_stats_df = pd.DataFrame(pitcher_stats)
    games_df = pd.DataFrame(games)
    
    # Run the model and get the daily picks
    daily_picks = run_prediction_model(games_df, team_stats_df, pitcher_stats_df)
    
    # Upsert the final predictions
    # A composite key of date and teams is best for daily picks
    upsert_data(supabase, 'daily_picks', daily_picks, 'pick_date,home_team,away_team')
    
    print("\n✅ Pipeline completed successfully.")

if __name__ == "__main__":
    main()
