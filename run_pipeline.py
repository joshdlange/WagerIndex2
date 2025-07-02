# run_pipeline.py
import os, sys, requests, pandas as pd, numpy as np
from supabase import create_client
from dotenv import load_dotenv
from datetime import datetime

# --- CONFIGURATION ---
load_dotenv()
SUPABASE_URL, SUPABASE_KEY = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}

# THE SPREADSHEET LOGIC: This is the heart of the model.
# Each stat has an API key, a DB column, a point value, and a rule for winning.
STAT_WEIGHTS = {
    'team_batting': [
        {'api_key': 'runs', 'db_col': 'runs_scored', 'points': 1.0, 'higher_is_better': True},
        {'api_key': 'avg', 'db_col': 'batting_avg', 'points': 0.75, 'higher_is_better': True},
        {'api_key': 'onBasePct', 'db_col': 'obp', 'points': 0.5, 'higher_is_better': True},
        {'api_key': 'sluggingPct', 'db_col': 'slugging', 'points': 0.5, 'higher_is_better': True},
    ],
    'team_pitching': [
        {'api_key': 'earnedRunAverage', 'db_col': 'era', 'points': 1.0, 'higher_is_better': False},
        {'api_key': 'walksAndHitsPerInningPitched', 'db_col': 'whip', 'points': 0.75, 'higher_is_better': False},
    ],
    'team_fielding': [
        {'api_key': 'fieldingPct', 'db_col': 'fielding_pct', 'points': 0.5, 'higher_is_better': True},
    ],
    'pitcher_stats': [
        {'api_key': 'ERA', 'db_col': 'era', 'points': 1.0, 'higher_is_better': False},
        {'api_key': 'WHIP', 'db_col': 'whip', 'points': 0.75, 'higher_is_better': False},
        {'api_key': 'K/9', 'db_col': 'k_per_9', 'points': 0.5, 'higher_is_better': True},
        {'api_key': 'BB/9', 'db_col': 'bb_per_9', 'points': 0.5, 'higher_is_better': False},
    ]
}

# --- UTILITY & DB FUNCTIONS ---
def get_supabase_client():
    if not SUPABASE_URL or not SUPABASE_KEY: print("❌ Fatal: Supabase secrets required."), sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def get_current_season_year():
    print(" Hitting ESPN API for official season year...")
    try:
        data = requests.get("https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard", timeout=15).json()
        s = data.get("season", {}); year, type = s.get("year"), s.get("type")
        if not year or not type: raise ValueError("API missing year/type")
        if type == 4: year -= 1; print(f"  -> Offseason. Using previous year ({year}).")
        else: print(f"  -> Active season: {year}.")
        return year
    except Exception as e: print(f"❌ CRITICAL FAILURE fetching year: {e}"), sys.exit(1)

def upsert_data(supabase, table_name, records, conflict_col):
    if not records: print(f"✅ INFO: No records to upsert for '{table_name}'."); return
    print(f"⬆️ Upserting {len(records)} records to '{table_name}'...")
    try:
        df = pd.DataFrame(records).replace([np.inf, -np.inf], np.nan)
        response = supabase.table(table_name).upsert(df.where(pd.notnull(df), None).to_dict('records'), on_conflict=conflict_col).execute()
        if not response.data: raise Exception(getattr(response, 'error', 'Unknown error'))
        print(f"✅ Success.")
    except Exception as e: print(f"❌ Supabase upsert for '{table_name}' failed: {e}"), sys.exit(1)

# --- PIPELINE STEPS ---
def step_1_teams(supabase):
    print("\n--- 1. Syncing Teams ---")
    try:
        data = requests.get(f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams", headers=HEADERS).json()
        records = [{'name': t['team']['displayName'], 'abbreviation': t['team']['abbreviation']} for t in data['sports'][0]['leagues'][0]['teams']]
        upsert_data(supabase, 'teams', records, 'abbreviation')
        db_teams = supabase.table('teams').select('id, abbreviation, name').execute().data
        return {t['abbreviation']: {'id': t['id'], 'name': t['name']} for t in db_teams}
    except Exception as e: print(f"❌ Fatal Error in Step 1: {e}"), sys.exit(1)

def step_2_team_stats(supabase, year, team_map):
    print(f"\n--- 2. Fetching Team Stats for {year} ---")
    stats = {abbr: {'team_id': info['id']} for abbr, info in team_map.items()}
    for group_name, group_id in {'batting': '10', 'pitching': '11', 'fielding': '12'}.items():
        try:
            url = f"https://site.api.espn.com/apis/v2/sports/baseball/mlb/seasons/{year}/types/2/groups/{group_id}/stats"
            data = requests.get(url, headers=HEADERS).json()
            stat_source = data if 'stats' in data else data.get('categories', [{}])[0]
            names = [s.get('abbreviation') for s in stat_source.get('stats', [])]
            for team_data in data.get('teams', []):
                abbr = team_data.get('team', {}).get('abbreviation')
                if abbr in stats:
                    for i, val in enumerate(team_data.get('stats', [])): stats[abbr][names[i]] = val
        except Exception as e: print(f"❌ Fatal Error fetching {group_name} stats: {e}"), sys.exit(1)
    
    records = []
    all_weighted_stats = STAT_WEIGHTS['team_batting'] + STAT_WEIGHTS['team_pitching'] + STAT_WEIGHTS['team_fielding']
    for abbr, s in stats.items():
        record = {'team_id': s['team_id']}
        for stat_info in all_weighted_stats:
            record[stat_info['db_col']] = s.get(stat_info['api_key'])
        record['updated_at'] = datetime.now().isoformat()
        records.append(record)
    upsert_data(supabase, 'team_stats', records, 'team_id')
    return pd.DataFrame([r for r in records if r['team_id'] is not None])

def step_3_pitcher_stats(supabase, year, team_map):
    print(f"\n--- 3. Fetching Pitcher Stats for {year} ---")
    try:
        url = f"https://site.api.espn.com/apis/v2/sports/baseball/mlb/seasons/{year}/types/2/stats?limit=1000"
        data = requests.get(url, headers=HEADERS).json()
        cat = next((c for c in data.get('categories', []) if c.get('name') == 'pitching'), None)
        if not cat: raise ValueError("Could not find 'pitching' category")
        names = [s.get('abbreviation') for s in cat.get('stats', [])]
        records = []
        for p_data in data.get('athletes', []):
            abbr = p_data.get('team', {}).get('abbreviation')
            if abbr in team_map:
                stats = {names[i]: val for i, val in enumerate(p_data.get('stats', []))}
                # Only include pitchers with innings pitched
                if float(stats.get('IP', 0)) > 0:
                    records.append({'name': p_data['athlete']['displayName'], 'team_id': team_map[abbr]['id'],
                                    'era': stats.get('ERA'), 'whip': stats.get('WHIP'),
                                    'k_per_9': stats.get('K/9'), 'bb_per_9': stats.get('BB/9'),
                                    'innings_pitched': stats.get('IP')})
        upsert_data(supabase, 'pitchers', records, 'name')
        return pd.DataFrame(records)
    except Exception as e: print(f"❌ Fatal Error fetching pitcher stats: {e}"), sys.exit(1)

def step_4_games(supabase):
    print("\n--- 4. Fetching Today's Games ---")
    try:
        data = requests.get("https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard", headers=HEADERS).json()
        records = []
        for event in data.get("events", []):
            comp = event["competitions"][0]
            home = next((c for c in comp["competitors"] if c["homeAway"] == "home"), {}); away = next((c for c in comp["competitors"] if c["homeAway"] == "away"), {})
            rec = {'game_date': event["date"].split("T")[0], 'game_id': event["id"],
                   'home_team_abbr': home.get("team", {}).get("abbreviation"), 'away_team_abbr': away.get("team", {}).get("abbreviation"),
                   'home_pitcher': home.get("probablePitcher", {}).get("athlete", {}).get("displayName"), 'away_pitcher': away.get("probablePitcher", {}).get("athlete", {}).get("displayName")}
            try:
                odds = next(o for o in comp.get('odds', []) if 'moneyLine' in o.get('homeTeamOdds', {}))
                rec['home_moneyline'], rec['away_moneyline'] = odds.get('homeTeamOdds',{}).get('moneyLine'), odds.get('awayTeamOdds',{}).get('moneyLine')
            except StopIteration: rec['home_moneyline'], rec['away_moneyline'] = None, None
            records.append(rec)
        upsert_data(supabase, 'games', records, 'game_id')
        return pd.DataFrame(records)
    except Exception as e: print(f"❌ Fatal Error fetching daily games: {e}"), sys.exit(1)

def step_5_model(supabase, games_df, team_stats_df, pitcher_stats_df, team_map):
    print("\n--- 5. Running Spreadsheet Logic ---")
    if games_df.empty: print("✅ No games scheduled."); return
    if team_stats_df.empty or pitcher_stats_df.empty: print("❌ Halting: Missing stats data."), sys.exit(1)

    abbr_to_id_map = {abbr: info['id'] for abbr, info in team_map.items()}
    predictions = []

    for _, game in games_df.iterrows():
        try:
            home_id, away_id = abbr_to_id_map.get(game['home_team_abbr']), abbr_to_id_map.get(game['away_team_abbr'])
            home_team_stats, away_team_stats = team_stats_df[team_stats_df['team_id'] == home_id].iloc[0], team_stats_df[team_stats_df['team_id'] == away_id].iloc[0]
            home_pitcher, away_pitcher = pitcher_stats_df[pitcher_stats_df['name'] == game['home_pitcher']], pitcher_stats_df[pitcher_stats_df['name'] == game['away_pitcher']]
            
            home_points, away_points = 0.0, 0.0

            # Team Head-to-Head Comparisons
            for category in ['team_batting', 'team_pitching', 'team_fielding']:
                for stat_info in STAT_WEIGHTS[category]:
                    db_col = stat_info['db_col']
                    home_val, away_val = home_team_stats.get(db_col, 0), away_team_stats.get(db_col, 0)
                    if home_val is None or away_val is None: continue
                    
                    if stat_info['higher_is_better']:
                        if home_val > away_val: home_points += stat_info['points']
                        elif away_val > home_val: away_points += stat_info['points']
                    else: # Lower is better
                        if home_val < away_val: home_points += stat_info['points']
                        elif away_val < home_val: away_points += stat_info['points']

            # Pitcher Head-to-Head Comparisons
            if not home_pitcher.empty and not away_pitcher.empty:
                home_p_stats, away_p_stats = home_pitcher.iloc[0], away_pitcher.iloc[0]
                for stat_info in STAT_WEIGHTS['pitcher_stats']:
                    db_col = stat_info['db_col']
                    home_val, away_val = home_p_stats.get(db_col, 0), away_p_stats.get(db_col, 0)
                    if home_val is None or away_val is None: continue

                    if stat_info['higher_is_better']:
                        if home_val > away_val: home_points += stat_info['points']
                        elif away_val > home_val: away_points += stat_info['points']
                    else: # Lower is better
                        if home_val < away_val: home_points += stat_info['points']
                        elif away_val < home_val: away_points += stat_info['points']

            # Final Score Calculation
            predicted_home_score = round(home_points / 10, 2)
            predicted_away_score = round(away_points / 10, 2)

            predictions.append({'game_id': game['game_id'], 'pick_date': game['game_date'],
                                'home_team': game['home_team_abbr'], 'away_team': game['away_team_abbr'],
                                'predicted_home_score': predicted_home_score, 'predicted_away_score': predicted_away_score,
                                'predicted_winner': game['home_team_abbr'] if predicted_home_score > predicted_away_score else game['away_team_abbr'],
                                'confidence_score': abs(predicted_home_score - predicted_away_score)})
        except Exception as e: print(f"⚠️ Error processing game {game['away_team_abbr']} vs {game['home_team_abbr']}: {e}")
    
    if predictions:
        picks_df = pd.DataFrame(predictions)
        # Placeholder for Pick of the Day logic, focusing on correct score generation first.
        picks_df['is_pick_of_day'] = False
        if not picks_df.empty:
            top_pick_index = picks_df['confidence_score'].idxmax()
            picks_df.loc[top_pick_index, 'is_pick_of_day'] = True
        
        upsert_data(supabase, 'daily_picks', picks_df.to_dict('records'), 'game_id')

def main():
    print("🚀 Starting WagerIndex Daily Pipeline (The Final Build)...")
    supabase = get_supabase_client()
    year = get_current_season_year()
    team_map = step_1_teams(supabase)
    team_stats_df = step_2_team_stats(supabase, year, team_map)
    pitcher_stats_df = step_3_pitcher_stats(supabase, year, team_map)
    games_df = step_4_games(supabase)
    step_5_model(supabase, games_df, team_stats_df, pitcher_stats_df, team_map)
    print("\n✅✅✅ Pipeline Completed Successfully ✅✅✅")

if __name__ == "__main__":
    main()
