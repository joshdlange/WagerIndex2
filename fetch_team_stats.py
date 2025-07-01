# fetch_team_stats.py
import os
import sys
import requests
import pandas as pd
import numpy as np
import io  # Required to fix the FutureWarning from pandas
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime
from utils import get_current_season_year

load_dotenv()

# --- Config ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}

def scrape_espn_table(url):
    """
    Directly scrapes and cleans HTML tables from a given ESPN stats URL.
    This version is hardened against common scraping issues.
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        
        # Use io.StringIO to address the FutureWarning
        tables = pd.read_html(io.StringIO(response.text))
        
        if len(tables) < 2:
            raise ValueError("Expected at least two stats tables (AL/NL), but found fewer.")
        
        # Combine the primary stats tables (usually the first two)
        df = pd.concat([tables[0], tables[1]], ignore_index=True)
        
        # --- ROBUSTNESS FIX #1: Filter out repeating header rows ---
        # The first column name changes, so we access it by position
        first_col_name = df.columns[0]
        # Remove any rows where the first column is the header text again (e.g., 'Team')
        df = df[df[first_col_name] != first_col_name]
        
        return df
    except Exception as e:
        print(f"❌ Failed to scrape or parse URL {url}: {e}")
        return None

def fetch_and_upsert_team_stats():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Fatal Error: SUPABASE_URL and SUPABASE_KEY secrets must be set.")
        sys.exit(1)

    try:
        SEASON_YEAR = get_current_season_year()
    except Exception:
        sys.exit(1)

    BATTING_URL = f"https://www.espn.com/mlb/stats/team/_/season/{SEASON_YEAR}"
    PITCHING_URL = f"https://www.espn.com/mlb/stats/team/_/view/pitching/season/{SEASON_YEAR}"
    FIELDING_URL = f"https://www.espn.com/mlb/stats/team/_/view/fielding/season/{SEASON_YEAR}"

    print(f"📊 Scraping web pages for {SEASON_YEAR} season stats...")
    
    batting_df = scrape_espn_table(BATTING_URL)
    pitching_df = scrape_espn_table(PITCHING_URL)
    fielding_df = scrape_espn_table(FIELDING_URL)

    if batting_df is None or pitching_df is None or fielding_df is None:
        print("❌ Fatal Error: Failed to scrape one or more stat categories. Aborting.")
        sys.exit(1)

    print("  -> Cleaning and merging scraped data...")
    # Rename the first column consistently
    batting_df = batting_df.rename(columns={batting_df.columns[0]: 'TeamNameRaw'})
    pitching_df = pitching_df.rename(columns={pitching_df.columns[0]: 'TeamNameRaw'})
    fielding_df = fielding_df.rename(columns={fielding_df.columns[0]: 'TeamNameRaw'})

    # --- THE DIRECT FIX for the AttributeError ---
    # 1. Convert the entire column to string type. This prevents the crash.
    #    NaNs and other types become string representations (e.g., 'nan').
    batting_df['TeamNameRaw'] = batting_df['TeamNameRaw'].astype(str)
    # 2. Use a more robust regex to remove the trailing abbreviation.
    #    This will not fail on 'nan' or other unexpected strings.
    batting_df['TeamName'] = batting_df['TeamNameRaw'].str.replace(r'[A-Z]{2,3}$', '', regex=True).str.strip()
    
    # Do the same for the other dataframes for consistency
    pitching_df['TeamName'] = pitching_df['TeamNameRaw'].astype(str).str.replace(r'[A-Z]{2,3}$', '', regex=True).str.strip()
    fielding_df['TeamName'] = fielding_df['TeamNameRaw'].astype(str).str.replace(r'[A-Z]{2,3}$', '', regex=True).str.strip()

    # Select and merge essential columns
    batting_essentials = batting_df[['TeamName', 'GP', 'R', 'H', 'HR', 'AVG', 'SO', 'BB']]
    pitching_essentials = pitching_df[['TeamName', 'ERA', 'WHIP']]
    fielding_essentials = fielding_df[['TeamName', 'E']]

    merged_1 = pd.merge(batting_essentials, pitching_essentials, on='TeamName', how='inner')
    final_df = pd.merge(merged_1, fielding_essentials, on='TeamName', how='inner')

    # Map team names to abbreviations
    try:
        team_data_url = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams"
        team_map_resp = requests.get(team_data_url, headers=HEADERS, timeout=10)
        team_map_resp.raise_for_status()
        teams_json = team_map_resp.json()['sports'][0]['leagues'][0]['teams']
        team_abbr_map = {team_info['team']['displayName']: team_info['team']['abbreviation'] for team_info in teams_json}
        final_df['team_abbr'] = final_df['TeamName'].map(team_abbr_map)
    except Exception as e:
        print(f"❌ Could not create team abbreviation map: {e}")
        sys.exit(1)
        
    final_df.dropna(subset=['team_abbr'], inplace=True)
    
    final_df = final_df.rename(columns={
        'GP': 'games_played', 'R': 'runs', 'H': 'hits', 'HR': 'home_runs', 'AVG': 'batting_average',
        'SO': 'strikeouts_batting', 'BB': 'walks_batting', 'E': 'errors'
    })

    # Sanitize for JSON compatibility
    final_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    final_records = final_df.where(pd.notnull(final_df), None).to_dict('records')
    for record in final_records:
        record['last_updated'] = datetime.now().isoformat()

    print(f"⬆️ Upserting {len(final_records)} teams' complete stats into Supabase...")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        response = supabase.table("team_stats").upsert(final_records, on_conflict="team_abbr").execute()
        if response.data:
            print("✅ Team stats upserted successfully.")
        else:
            print(f"❌ Supabase Error: {getattr(response, 'error', 'Unknown error')}")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Supabase upsert failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_and_upsert_team_stats()
