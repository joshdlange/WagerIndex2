# run_pipeline.py
import os
import sys
import requests
import pandas as pd
import numpy as np
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime
from pybaseball import pitching_stats as pybaseball_pitching_stats

# --- CONFIGURATION ---
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
WEIGHTS = {'batting': 0.40, 'pitching': 0.30, 'bullpen': 0.20, 'defense': 0.10}

# --- DATABASE & UTILS ---
def get_supabase_client():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Fatal Error: SUPABASE_URL and SUPABASE_KEY secrets must be set.")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def get_current_season_year():
    print(" Hitting ESPN API to get the official current season year...")
    try:
        url = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard"
        response = requests.get(url, timeout=15).json()
        season_data = response.get("season", {})
        api_year, season_type = season_data.get("year"), season_data.get("type")
        if not api_year or not season_type: raise ValueError("API response missing 'year' or 'type'.")

        if season_type == 4: # 4 = Offseason
            year = api_year - 1
            print(f"  -> API reports offseason. Using previous year ({year}) for stats.")
            return year
        else:
            print(f"  -> API reports active season: {api_year}.")
            return api_year
    except Exception as e:
        print(f"❌ CRITICAL FAILURE fetching year from ESPN API ({e}). Aborting.")
        sys.exit(1)

def upsert_data(supabase, table_name, records, conflict_col):
    if not records:
        print(f"✅ INFO: No new records to upsert for table '{table_name}'.")
        return
    print(f"⬆️ Upserting {len(records)} records to '{table_name}'...")
    try:
        # Sanitize data: replace NaN/inf with None for JSON compliance
        df = pd.DataFrame(records)
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        final_records = df.where(pd.notnull(df), None).to_dict('records')
        
        response = supabase.table(table_name).upsert(final_records, on_conflict=conflict_col).execute()
        if not response.data: raise Exception(getattr(response, 'error', 'Unknown error'))
        print(f"✅ Successfully upserted data to '{table_name}'.")
    except Exception as e:
        print(f"❌ Supabase upsert for '{table_name}' failed: {e}")
        sys.exit(1)

# --- PIPELINE STEPS ---

def fetch_teams(supabase):
    print("\n--- 1. Synchronizing Teams Table ---")
    try:
        teams_url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams"
        teams_json = requests.get(teams_url, headers=HEADERS).json()['sports'][0]['leagues'][0]['teams']
        teams_records = [{'name': team['team']['displayName'], 'abbreviation': team['team']['abbreviation']} for team in teams_json]
        upsert_data(supabase, 'teams', teams_records, 'abbreviation')
        
        db_teams = supabase.table('teams').select('id, abbreviation').execute().data
        return {team['abbreviation']: team['id'] for team in db_teams}
    except Exception as e:
        print(f"❌ Fatal Error in Step 1 (Teams): {e}")
        sys.exit(1)

def fetch_team_stats(supabase, year, team_id_map):
    print(f"\n--- 2. Fetching Team Stats for {year} ---")
    stats_map = {abbr: {'team_id': team_id} for abbr, team_id in team_id_map.items()}
    
    # These are the correct group IDs from the real API
    # 10=batting, 11=pitching, 12=fielding
    for category, group_id in {'batting': '10', 'pitching': '11', 'fielding': '12'}.items():
        print(f"  -> Fetching {category} data...")
        try:
            url = f"https://site.api.espn.com/apis/v2/sports/baseball/mlb/seasons/{year}/types/2/groups/{group_id}/stats"
            response = requests.get(url, headers=HEADERS).json()
            stat_names = [s['name'] for s in response['stats']]
            
            for team_data in response['teams']:
                team_abbr = team_data['team']['abbreviation']
                if team_abbr in stats_map:
                    for i, stat_value in enumerate(team_data['stats']):
                        stat_name = stat_names[i]
                        stats_map[team_abbr][stat_name] = stat_value
        except Exception as e:
            print(f"❌ Fatal Error fetching {category} stats: {e}")
            sys.exit(1)
            
    # Map to your exact schema
    records = []
    for abbr, stats in stats_map.items():
        records.append({
            'team_id': stats.get('team_id'),
            'batting_avg': stats.get('avg'), 'obp': stats.get('onBasePlusSlugging'), # Note: ESPN combines OBP+SLG into OPS
            'slugging': stats.get('slugging'), 'runs_per_game': stats.get('runs'),
            'era': stats.get('earnedRunAverage'), 'whip': stats.get('walksAndHitsPerInningPitched'),
            'fielding_pct': stats.get('fieldingPct'), 'errors_per_game': stats.get('errors'),
            'updated_at': datetime.now().isoformat()
        })
        
    upsert_data(supabase, 'team_stats', records, 'team_id')
    return pd.DataFrame(records)

def fetch_pitcher_stats(supabase, year, team_id_map):
    print(f"\n--- 3. Fetching Pitcher Stats for {year} ---")
    try:
        pitchers_df = pybaseball_pitching_stats(year)
        filtered_df = pitchers_df[pitchers_df['IP'] >= 10].copy()
        
        final_records = []
        for _, row in filtered_df.iterrows():
            team_abbr = row.get('Team')
            if team_abbr in team_id_map:
                final_records.append({
                    'name': row.get('Name'),
                    'team_id': team_id_map[team_abbr], # THE FIX: Using the correct team_id
                    'era': row.get('ERA'), 'whip': row.get('WHIP'),
                    'k9': row.get('K/9'), 'bb9': row.get('BB/9'),
                    'innings_pitched': row.get('IP')
                })
        upsert_data(supabase, 'pitchers', final_records, 'name')
        return pd.DataFrame(final_records)
    except Exception as e:
        print(f"❌ Fatal Error fetching pitcher stats: {e}")
        sys.exit(1)

def run_prediction_model(games_df, team_stats_df, pitcher_stats_df):
    # This is a placeholder as no games are fetched yet.
    # This logic will be implemented once the data pipeline is proven stable.
    print("\n--- 4. Running Prediction Model (STUB) ---")
    print("✅ Data pipeline successful. Model logic will be implemented next.")
    return []

# --- MAIN WORKFLOW ---
def main():
    print("🚀 Starting WagerIndex Daily Pipeline (v3.0)...")
    supabase = get_supabase_client()
    
    year = get_current_season_year()
    
    # Step 1: Get Team IDs. This is required for foreign keys.
    team_id_map = fetch_teams(supabase)
    
    # Step 2 & 3 can run now that we have the team_id_map
    team_stats_df = fetch_team_stats(supabase, year, team_id_map)
    pitcher_stats_df = fetch_pitcher_stats(supabase, year, team_id_map)

    # Deferring game fetch and model run until the core data is stable.
    # games_df = fetch_daily_games(supabase, team_id_map)
    # daily_picks = run_prediction_model(games_df, team_stats_df, pitcher_stats_df)
    # upsert_data(supabase, 'daily_picks', daily_picks, 'pick_date,home_team,away_team')
    
    print("\n✅✅✅ Core Data Ingestion Pipeline Completed Successfully ✅✅✅")

if __name__ == "__main__":
    main()
