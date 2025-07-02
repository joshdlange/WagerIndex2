# run_pipeline.py
import os, sys, requests, pandas as pd, numpy as np
from supabase import create_client
from dotenv import load_dotenv
from datetime import datetime

# --- CONFIGURATION ---
load_dotenv()
SUPABASE_URL, SUPABASE_KEY = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
WEIGHTS = {'batting': 0.40, 'pitching': 0.30, 'bullpen': 0.20, 'defense': 0.10}

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
    # THE FIX: This entire block is rewritten to be robust.
    for group_name, group_id in {'batting': '10', 'pitching': '11', 'fielding': '12'}.items():
        print(f"  -> Fetching {group_name} data...")
        try:
            url = f"https://site.api.espn.com/apis/v2/sports/baseball/mlb/seasons/{year}/types/2/groups/{group_id}/stats"
            data = requests.get(url, headers=HEADERS).json()
            # NO MORE ASSUMPTIONS: Take the first category available, regardless of name.
            if not data.get('categories'): raise ValueError(f"API response for {group_name} is missing 'categories' key.")
            stat_category = data['categories'][0]
            names = [s.get('abbreviation') for s in stat_category.get('stats', [])]
            for team_data in data.get('teams', []):
                abbr = team_data.get('team', {}).get('abbreviation')
                if abbr in stats:
                    for i, val in enumerate(team_data.get('stats', [])):
                        stats[abbr][names[i]] = val
        except Exception as e: print(f"❌ Fatal Error fetching stat group {group_id}: {e}"), sys.exit(1)
    
    records = [{'team_id': s.get('team_id'), 'wins': s.get('W'), 'losses': s.get('L'),
                'batting_avg': s.get('AVG'), 'obp': s.get('OBP'), 'slugging': s.get('SLG'),
                'runs_per_game': s.get('RPG'), 'era': s.get('ERA'),
                'whip': s.get('WHIP'), 'fielding_pct': s.get('FPCT'),
                'errors_per_game': s.get('E'), 'updated_at': datetime.now().isoformat()}
               for abbr, s in stats.items()]
    upsert_data(supabase, 'team_stats', records, 'team_id')
    return pd.DataFrame([r for r in records if r['team_id'] is not None])

def step_3_pitcher_stats(supabase, year, team_map):
    print(f"\n--- 3. Fetching Pitcher Stats for {year} ---")
    try:
        # THE FIX: I am abandoning pybaseball. It is an unreliable dependency.
        # ALL data now comes from the unified ESPN API source.
        url = f"https://site.api.espn.com/apis/v2/sports/baseball/mlb/seasons/{year}/types/2/stats?limit=1000"
        data = requests.get(url, headers=HEADERS).json()
        # NO MORE ASSUMPTIONS: Search for the 'pitching' category by name.
        cat = next((c for c in data.get('categories', []) if c.get('name') == 'pitching'), None)
        if not cat: raise ValueError("Could not find 'pitching' category in pitcher stats API response")
        names = [s.get('abbreviation') for s in cat.get('stats', [])]
        records = []
        for p_data in data.get('athletes', []):
            abbr = p_data.get('team', {}).get('abbreviation')
            if abbr in team_map:
                stats = {names[i]: val for i, val in enumerate(p_data.get('stats', []))}
                # Only add pitchers who have pitched at least one inning
                if stats.get('IP', 0) > 0:
                    records.append({'name': p_data['athlete']['displayName'], 'team_id': team_map[abbr]['id'],
                                    'era': stats.get('ERA'), 'whip': stats.get('WHIP'), 'k9': stats.get('K/9'),
                                    'bb9': stats.get('BB/9'), 'innings_pitched': stats.get('IP')})
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
                rec['home_moneyline'], rec['away_moneyline'] = odds.get('homeTeamOdds',{}), odds.get('awayTeamOdds',{})
            except StopIteration: rec['home_moneyline'], rec['away_moneyline'] = None, None
            records.append(rec)
        upsert_data(supabase, 'games', records, 'game_id')
        return pd.DataFrame(records)
    except Exception as e: print(f"❌ Fatal Error fetching daily games: {e}"), sys.exit(1)

def step_5_model(supabase, games_df, team_stats_df, pitcher_stats_df, team_map):
    print("\n--- 5. Running Prediction Model & Upserting Picks ---")
    if games_df.empty: print("✅ No games scheduled."); return
    if team_stats_df.empty or pitcher_stats_df.empty: print("❌ Halting: Missing stats data."); sys.exit(1)

    abbr_to_id_map = {abbr: info['id'] for abbr, info in team_map.items()}
    def norm(s, asc=True): return s.rank(method='max', ascending=asc, pct=True) * 100
    
    team_stats_df['batting_score'] = norm(team_stats_df['batting_avg'])
    team_stats_df['defense_score'] = norm(team_stats_df['fielding_pct'])
    team_stats_df['bullpen_score'] = norm(team_stats_df['era'], asc=False)
    pitcher_stats_df['pitching_score'] = (norm(pitcher_stats_df['era'], asc=False) + norm(pitcher_stats_df['whip'], asc=False)) / 2
    avg_pitcher_score = pitcher_stats_df['pitching_score'].mean()

    predictions = []
    for _, game in games_df.iterrows():
        try:
            home_id, away_id = abbr_to_id_map.get(game['home_team_abbr']), abbr_to_id_map.get(game['away_team_abbr'])
            home_stats, away_stats = team_stats_df[team_stats_df['team_id'] == home_id].iloc[0], team_stats_df[team_stats_df['team_id'] == away_id].iloc[0]
            home_p, away_p = pitcher_stats_df[pitcher_stats_df['name'] == game['home_pitcher']], pitcher_stats_df[pitcher_stats_df['name'] == game['away_pitcher']]
            home_p_score = home_p['pitching_score'].iloc[0] if not home_p.empty else avg_pitcher_score
            away_p_score = away_p['pitching_score'].iloc[0] if not away_p.empty else avg_pitcher_score

            home_w_score = (home_stats['batting_score'] * WEIGHTS['batting'] + home_p_score * WEIGHTS['pitching'] + home_stats['bullpen_score'] * WEIGHTS['bullpen'] + home_stats['defense_score'] * WEIGHTS['defense'])
            away_w_score = (away_stats['batting_score'] * WEIGHTS['batting'] + away_p_score * WEIGHTS['pitching'] + away_stats['bullpen_score'] * WEIGHTS['bullpen'] + away_stats['defense_score'] * WEIGHTS['defense'])
            
            predictions.append({'game_id': game['game_id'], 'pick_date': game['game_date'],
                                'home_team': game['home_team_abbr'], 'away_team': game['away_team_abbr'],
                                'predicted_home_score': round(home_w_score / 10, 2), 'predicted_away_score': round(away_w_score / 10, 2),
                                'predicted_winner': game['home_team_abbr'] if home_w_score > away_w_score else game['away_team_abbr'],
                                'confidence_score': abs(home_w_score - away_w_score),
                                'home_moneyline': game['home_moneyline'], 'away_moneyline': game['away_moneyline']})
        except Exception as e: print(f"⚠️ Error processing game {game['away_team_abbr']} vs {game['home_team_abbr']}: {e}")
    
    if predictions:
        picks_df = pd.DataFrame(predictions)
        picks_df['margin_rank'] = picks_df['confidence_score'].rank(ascending=False)
        picks_df['highest_moneyline'] = pd.to_numeric(picks_df['home_moneyline'], errors='coerce').fillna(0)
        away_ml = pd.to_numeric(picks_df['away_moneyline'], errors='coerce').fillna(0)
        picks_df['highest_moneyline'] = picks_df['highest_moneyline'].combine(away_ml, max)
        picks_df['moneyline_rank'] = picks_df['highest_moneyline'].rank(ascending=False)
        picks_df['avg_rank'] = (picks_df['margin_rank'] + picks_df['moneyline_rank']) / 2
        picks_df['final_rank'] = picks_df['avg_rank'].rank(ascending=True)
        picks_df['is_pick_of_day'] = picks_df['final_rank'] == 1.0
        
        final_picks = picks_df[['game_id', 'pick_date', 'home_team', 'away_team', 'predicted_winner', 'confidence_score', 'is_pick_of_day', 'predicted_home_score', 'predicted_away_score']].to_dict('records')
        upsert_data(supabase, 'daily_picks', final_picks, 'game_id')

def main():
    print("🚀 Starting WagerIndex Daily Pipeline (v12.0 - Final Attempt)...")
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
