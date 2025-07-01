# run_pipeline.py
import os
import sys
import requests
import pandas as pd
import numpy as np
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime
from pybaseball import pitching_stats

# --- CONFIGURATION ---
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
WEIGHTS = {'batting': 0.40, 'pitching': 0.30, 'bullpen': 0.20, 'defense': 0.10}

# --- DATABASE & UTILITY FUNCTIONS ---

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
        if season_type == 4:
            year = api_year - 1
            print(f"  -> API reports offseason. Using previous year ({year}) for stats.")
            return year
        print(f"  -> API reports active season: {api_year}.")
        return api_year
    except Exception as e:
        print(f"❌ CRITICAL FAILURE fetching year from ESPN API ({e}). Aborting.")
        sys.exit(1)

def upsert_data(supabase, table_name, records, conflict_col):
    if not records:
        print(f"✅ INFO: No new records to upsert for table '{table_name}'.")
        return
    print(f"⬆️ Preparing to upsert {len(records)} records to '{table_name}'...")
    try:
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

def step_1_fetch_and_get_teams(supabase):
    print("\n--- 1. Synchronizing Teams Table ---")
    try:
        teams_url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams"
        teams_json = requests.get(teams_url, headers=HEADERS).json()['sports'][0]['leagues'][0]['teams']
        teams_records = [{'name': team['team']['displayName'], 'abbreviation': team['team']['abbreviation']} for team in teams_json]
        upsert_data(supabase, 'teams', teams_records, 'abbreviation')
        
        db_teams = supabase.table('teams').select('id, abbreviation, name').execute().data
        return {team['abbreviation']: {'id': team['id'], 'name': team['name']} for team in db_teams}
    except Exception as e:
        print(f"❌ Fatal Error in Step 1 (Teams): {e}")
        sys.exit(1)

def step_2_fetch_team_stats(supabase, year, team_map):
    print(f"\n--- 2. Fetching Team Stats for {year} ---")
    stats_map = {abbr: {'team_name': info['name']} for abbr, info in team_map.items()}
    
    for category, group_id in {'batting': '10', 'pitching': '11', 'fielding': '12'}.items():
        print(f"  -> Fetching {category} data...")
        try:
            url = f"https://site.api.espn.com/apis/v2/sports/baseball/mlb/seasons/{year}/types/2/groups/{group_id}/stats"
            response = requests.get(url, headers=HEADERS, timeout=15).json()
            stat_category = response.get('categories', [{}])[0]
            stat_names = [s.get('name') for s in stat_category.get('stats', [])]

            for team_data in response.get('teams', []):
                team_abbr = team_data.get('team', {}).get('abbreviation')
                if team_abbr in stats_map:
                    for i, stat_value in enumerate(team_data.get('stats', [])):
                        stats_map[team_abbr][stat_names[i]] = stat_value
        except Exception as e:
            print(f"❌ Fatal Error fetching {category} stats: {e}")
            sys.exit(1)
            
    # Map the fetched API keys to the EXACT schema columns defined in the SQL script.
    records = []
    for abbr, stats in stats_map.items():
        records.append({
            'team_name': stats.get('team_name'),
            'wins': stats.get('wins'), 'losses': stats.get('losses'),
            'batting_avg': stats.get('avg'), 'obp': stats.get('onBasePct'),
            'slugging': stats.get('sluggingPct'), 'runs_per_game': stats.get('runsPerGame'),
            'era': stats.get('earnedRunAverage'), 'whip': stats.get('walksAndHitsPerInningPitched'),
            'fielding_pct': stats.get('fieldingPct'), 'errors_per_game': stats.get('errors'),
            'updated_at': datetime.now().isoformat()
        })
        
    upsert_data(supabase, 'team_stats', records, 'team_name')
    return pd.DataFrame(records)

def step_3_fetch_pitcher_stats(supabase, year, team_map):
    print(f"\n--- 3. Fetching Pitcher Stats for {year} ---")
    try:
        pitchers_df = pitching_stats(year)
        filtered_df = pitchers_df[pitchers_df['IP'] >= 10].copy()
        
        final_records = []
        for _, row in filtered_df.iterrows():
            team_abbr = row.get('Team')
            if team_abbr in team_map:
                final_records.append({
                    'name': row.get('Name'), 'team_id': team_map[team_abbr]['id'],
                    'era': row.get('ERA'), 'whip': row.get('WHIP'),
                    'k9': row.get('K/9'), 'bb9': row.get('BB/9'),
                    'innings_pitched': row.get('IP')
                })
        upsert_data(supabase, 'pitchers', final_records, 'name')
        return pd.DataFrame(final_records)
    except Exception as e:
        print(f"❌ Fatal Error fetching pitcher stats: {e}")
        sys.exit(1)

def step_4_fetch_daily_games(supabase):
    print("\n--- 4. Fetching Today's Games ---")
    try:
        url = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard"
        response = requests.get(url, headers=HEADERS).json()
        
        games_records = []
        for event in response.get("events", []):
            comp = event["competitions"][0]
            home = next((c for c in comp["competitors"] if c["homeAway"] == "home"), {})
            away = next((c for c in comp["competitors"] if c["homeAway"] == "away"), {})
            
            # Match the 'games' table schema perfectly
            record = {'game_date': event.get("date", "").split("T")[0],
                      'game_id': event.get("id"),
                      'home_team_abbr': home.get("team", {}).get("abbreviation"),
                      'away_team_abbr': away.get("team", {}).get("abbreviation"),
                      'home_pitcher': home.get("probablePitcher", {}).get("athlete", {}).get("displayName"),
                      'away_pitcher': away.get("probablePitcher", {}).get("athlete", {}).get("displayName")}
            try:
                odds = next(o for o in comp.get('odds', []) if 'moneyLine' in o.get('homeTeamOdds', {}))
                record['home_moneyline'] = odds.get('homeTeamOdds', {}).get('moneyLine')
                record['away_moneyline'] = odds.get('awayTeamOdds', {}).get('moneyLine')
            except StopIteration: record['home_moneyline'], record['away_moneyline'] = None, None
            games_records.append(record)
            
        upsert_data(supabase, 'games', games_records, 'game_id')
        return pd.DataFrame(games_records)
    except Exception as e:
        print(f"❌ Fatal Error fetching daily games: {e}")
        sys.exit(1)

def step_5_run_model_and_upsert_picks(supabase, games_df, team_stats_df, pitcher_stats_df, team_map):
    print("\n--- 5. Running Prediction Model & Upserting Picks ---")
    if games_df.empty: print("✅ No games scheduled for today. Halting model."); return
    if team_stats_df.empty or 'batting_avg' not in team_stats_df.columns: print("❌ Halting model: team_stats data is missing or malformed."); sys.exit(1)
    if pitcher_stats_df.empty: print("✅ No pitchers met minimum IP. Halting model."); return

    name_to_abbr_map = {info['name']: abbr for abbr, info in team_map.items()}
    team_stats_df['team_abbr'] = team_stats_df['team_name'].map(name_to_abbr_map)

    def normalize_stat(series, ascending=True): return series.rank(method='max', ascending=ascending, pct=True) * 100
    
    team_stats_df['batting_score'] = normalize_stat(team_stats_df['batting_avg'], ascending=True)
    team_stats_df['defense_score'] = normalize_stat(team_stats_df['fielding_pct'], ascending=True)
    team_stats_df['bullpen_score'] = normalize_stat(team_stats_df['era'], ascending=False)
    pitcher_stats_df['pitching_score'] = (normalize_stat(pitcher_stats_df['era'], ascending=False) + normalize_stat(pitcher_stats_df['whip'], ascending=False)) / 2
    league_avg_pitcher_score = pitcher_stats_df['pitching_score'].mean()

    predictions = []
    for _, game in games_df.iterrows():
        try:
            away_team_data = team_stats_df[team_stats_df['team_abbr'] == game['away_team_abbr']].iloc[0]
            home_team_data = team_stats_df[team_stats_df['team_abbr'] == game['home_team_abbr']].iloc[0]
            away_pitcher_data = pitcher_stats_df[pitcher_stats_df['name'] == game['away_pitcher']]
            home_pitcher_data = pitcher_stats_df[pitcher_stats_df['name'] == game['home_pitcher']]
            
            away_pitching_score = away_pitcher_data['pitching_score'].iloc[0] if not away_pitcher_data.empty else league_avg_pitcher_score
            home_pitching_score = home_pitcher_data['pitching_score'].iloc[0] if not home_pitcher_data.empty else league_avg_pitcher_score

            away_score = (away_team_data['batting_score'] * WEIGHTS['batting'] + away_pitching_score * WEIGHTS['pitching'] + away_team_data['bullpen_score'] * WEIGHTS['bullpen'] + away_team_data['defense_score'] * WEIGHTS['defense'])
            home_score = (home_team_data['batting_score'] * WEIGHTS['batting'] + home_pitching_score * WEIGHTS['pitching'] + home_team_data['bullpen_score'] * WEIGHTS['bullpen'] + home_team_data['defense_score'] * WEIGHTS['defense'])

            predictions.append({'pick_date': game['game_date'], 'home_team': game['home_team_abbr'],
                                'away_team': game['away_team_abbr'],
                                'predicted_winner': game['home_team_abbr'] if home_score > away_score else game['away_team_abbr'],
                                'confidence_score': abs(home_score - away_score)})
        except Exception as e: print(f"⚠️ Error processing game {game['away_team_abbr']} vs {game['home_team_abbr']}: {e}")
    
    if predictions:
        max_confidence_pick = max(predictions, key=lambda x: x['confidence_score'])
        for pick in predictions: pick['is_pick_of_day'] = (pick == max_confidence_pick)
        upsert_data(supabase, 'daily_picks', predictions, 'pick_date,home_team,away_team')

# --- MAIN WORKFLOW ---
def main():
    print("🚀 Starting WagerIndex Daily Pipeline (v7.0 - Final Build)...")
    supabase = get_supabase_client()
    year = get_current_season_year()
    
    team_map = step_1_fetch_and_get_teams(supabase)
    team_stats_df = step_2_fetch_team_stats(supabase, year, team_map)
    pitcher_stats_df = step_3_fetch_pitcher_stats(supabase, year, team_map)
    games_df = step_4_fetch_daily_games(supabase)
    step_5_run_model_and_upsert_picks(supabase, games_df, team_stats_df, pitcher_stats_df, team_map)
    
    print("\n✅✅✅ Pipeline Completed Successfully ✅✅✅")

if __name__ == "__main__":
    main()
