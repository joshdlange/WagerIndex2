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

# --- UTILITY AND DATABASE FUNCTIONS ---

def get_supabase_client():
    """Establishes and returns a Supabase client, exiting on failure."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Fatal Error: SUPABASE_URL and SUPABASE_KEY secrets must be set.")
        sys.exit(1)
    print("✅ Supabase client initialized.")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def get_current_season_year():
    """Gets the correct, active MLB season year from the ESPN API, handling offseason logic."""
    print(" Hitting ESPN API to get the official current season year...")
    try:
        url = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard"
        response = requests.get(url, timeout=15).json()
        season_data = response.get("season", {})
        api_year, season_type = season_data.get("year"), season_data.get("type")
        if not api_year or not season_type:
            raise ValueError("API response missing 'year' or 'type'.")

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
    """Sanitizes and upserts a list of records to a Supabase table."""
    if not records:
        print(f"✅ INFO: No new records to upsert for table '{table_name}'.")
        return
    
    print(f"⬆️ Preparing to upsert {len(records)} records to '{table_name}'...")
    try:
        df = pd.DataFrame(records)
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        final_records = df.where(pd.notnull(df), None).to_dict('records')
        
        response = supabase.table(table_name).upsert(final_records, on_conflict=conflict_col).execute()
        if not response.data:
            raise Exception(getattr(response, 'error', 'Unknown error'))
        print(f"✅ Successfully upserted data to '{table_name}'.")
    except Exception as e:
        print(f"❌ Supabase upsert for '{table_name}' failed: {e}")
        sys.exit(1)

# --- PIPELINE STEPS ---

def step_1_fetch_and_get_teams(supabase):
    """Synchronizes the 'teams' table and returns a map of {abbr: id}."""
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

def step_2_fetch_team_stats(supabase, year, team_id_map):
    """Fetches stats from the real ESPN API and maps them to the user's schema."""
    print(f"\n--- 2. Fetching Team Stats for {year} ---")
    stats_map = {abbr: {'team_id': team_id} for abbr, team_id in team_id_map.items()}
    
    # Correct group IDs: 10=batting, 11=pitching, 12=fielding
    for category, group_id in {'batting': '10', 'pitching': '11', 'fielding': '12'}.items():
        print(f"  -> Fetching {category} data...")
        try:
            url = f"https://site.api.espn.com/apis/v2/sports/baseball/mlb/seasons/{year}/types/2/groups/{group_id}/stats"
            response = requests.get(url, headers=HEADERS, timeout=15).json()
            
            # THE FIX: Correctly navigate the JSON to find stat names and values.
            # The structure is response['categories'][0]['stats'] for names.
            stat_category = response.get('categories', [{}])[0]
            stat_names = [s.get('name') for s in stat_category.get('stats', [])]

            for team_data in response.get('teams', []):
                team_abbr = team_data.get('team', {}).get('abbreviation')
                if team_abbr in stats_map:
                    for i, stat_value in enumerate(team_data.get('stats', [])):
                        stat_name_key = stat_names[i]
                        stats_map[team_abbr][stat_name_key] = stat_value
        except Exception as e:
            print(f"❌ Fatal Error fetching {category} stats: {e}")
            sys.exit(1)
            
    # Map the fetched API keys to the exact schema columns provided by the user.
    records = []
    for abbr, stats in stats_map.items():
        records.append({
            'team_id': stats.get('team_id'),
            'batting_avg': stats.get('avg'), 'obp': stats.get('onBasePct'),
            'slugging': stats.get('sluggingPct'), 'runs_per_game': stats.get('runs'),
            'era': stats.get('earnedRunAverage'), 'whip': stats.get('walksAndHitsPerInningPitched'),
            'fielding_pct': stats.get('fieldingPct'), 'errors_per_game': stats.get('errors'),
            'updated_at': datetime.now().isoformat()
        })
        
    upsert_data(supabase, 'team_stats', records, 'team_id')
    return pd.DataFrame(records)

def step_3_fetch_pitcher_stats(supabase, year, team_id_map):
    """Fetches pitcher stats and correctly uses the team_id foreign key."""
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
                    'team_id': team_id_map[team_abbr], # Correctly maps abbreviation to UUID
                    'era': row.get('ERA'), 'whip': row.get('WHIP'),
                    'k9': row.get('K/9'), 'bb9': row.get('BB/9'),
                    'innings_pitched': row.get('IP')
                })
        upsert_data(supabase, 'pitchers', final_records, 'name')
        return pd.DataFrame(final_records)
    except Exception as e:
        print(f"❌ Fatal Error fetching pitcher stats: {e}")
        sys.exit(1)

def step_4_and_5_fetch_games_and_run_model():
    """Placeholder for the final steps once data ingestion is confirmed stable."""
    print("\n--- 4 & 5. Fetching Games & Running Model (STUB) ---")
    print("✅ Core Data Ingestion successful. Model logic will be implemented in the next step.")
    return

# --- MAIN WORKFLOW ---
def main():
    print("🚀 Starting WagerIndex Daily Pipeline (v4.0 - Final)...")
    supabase = get_supabase_client()
    
    year = get_current_season_year()
    
    # Step 1: Synchronize teams and get the ID map.
    team_id_map = step_1_fetch_and_get_teams(supabase)
    
    # Step 2: Fetch team stats using the correct API structure.
    team_stats_df = step_2_fetch_team_stats(supabase, year, team_id_map)
    
    # Step 3: Fetch pitcher stats and link them with the correct team_id.
    pitcher_stats_df = step_3_fetch_pitcher_stats(supabase, year, team_id_map)

    # Subsequent steps are stubbed out to prove the data pipeline works first.
    step_4_and_5_fetch_games_and_run_model()
    
    print("\n✅✅✅ Core Data Ingestion Pipeline Completed Successfully ✅✅✅")

if __name__ == "__main__":
    main()
