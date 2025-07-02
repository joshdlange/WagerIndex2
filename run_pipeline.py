# run_core_stats.py
import os, sys, requests, pandas as pd
from supabase import create_client
from dotenv import load_dotenv
from datetime import datetime

# --- CONFIGURATION ---
load_dotenv()
SUPABASE_URL, SUPABASE_KEY = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}

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
        response = supabase.table(table_name).upsert(records, on_conflict=conflict_col).execute()
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
        db_teams = supabase.table('teams').select('id, abbreviation').execute().data
        return {t['abbreviation']: t['id'] for t in db_teams}
    except Exception as e: print(f"❌ Fatal Error in Step 1: {e}"), sys.exit(1)

def step_2_core_stats(supabase, year, team_map):
    print(f"\n--- 2. Fetching Core Stat (Batting Average) for {year} ---")
    try:
        url = f"https://site.api.espn.com/apis/v2/sports/baseball/mlb/seasons/{year}/types/2/teams?limit=100"
        data = requests.get(url, headers=HEADERS).json()
        records = []
        for item in data.get("items", []):
            team_info = item.get("team", {})
            abbr = team_info.get("abbreviation")
            if abbr in team_map:
                stats = {s["name"]: s.get("value", 0) for s in team_info.get("stats", [])}
                records.append({
                    'team_id': team_map[abbr],
                    'batting_avg': stats.get('avg'),
                    'updated_at': datetime.now().isoformat()
                })
        upsert_data(supabase, 'team_stats', records, 'team_id')
    except Exception as e: print(f"❌ Fatal Error fetching team stats: {e}"), sys.exit(1)

# --- MAIN WORKFLOW ---
def main():
    print("🚀 Starting WagerIndex Core Stats Pipeline...")
    supabase = get_supabase_client()
    year = get_current_season_year()
    team_map = step_1_teams(supabase)
    step_2_core_stats(supabase, year, team_map)
    print("\n✅✅✅ Core Stats Pipeline Completed Successfully ✅✅✅")

if __name__ == "__main__":
    main()
