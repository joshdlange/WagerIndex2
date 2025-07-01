# fetch_team_stats.py
import os
import sys
import requests
import pandas as pd
import numpy as np
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime
from utils import get_current_season_year

load_dotenv()

# --- Config ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def fetch_espn_stats_by_category(year, category):
    """
    Helper function to fetch stats for a specific category (batting, pitching, fielding)
    from the correct, working ESPN API endpoints.
    """
    # These are the real API endpoints used by the ESPN website
    API_URL = f"https://site.api.espn.com/apis/v2/sports/baseball/mlb/seasons/{year}/types/2/groups/10/stats?lang=en®ion=us&contentorigin=espn"
    
    print(f"  -> Fetching {category} stats for {year}...")
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()
        
        # Find the correct category within the JSON response
        category_data = next((c for c in data['stats'] if c['name'] == category), None)
        if not category_data or not category_data.get("stats"):
            raise ValueError(f"Category '{category}' not found in API response.")
            
        # Convert the list of stats into a pandas DataFrame
        df = pd.DataFrame(category_data["stats"])
        df['team_id'] = [item['team']['id'] for item in data['teams']]
        df['team_abbr'] = [item['team']['abbreviation'] for item in data['teams']]
        return df
        
    except Exception as e:
        print(f"❌ Failed to fetch or parse {category} stats: {e}")
        return None

def fetch_and_upsert_team_stats():
    """
    Fetches team stats by making separate API calls for batting, pitching, and fielding,
    then merges them into a single, complete dataset.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Fatal Error: SUPABASE_URL and SUPABASE_KEY secrets must be set.")
        sys.exit(1)

    try:
        SEASON_YEAR = get_current_season_year()
    except Exception:
        sys.exit(1)

    print(f"📊 Fetching comprehensive team stats for {SEASON_YEAR} from ESPN's internal APIs...")

    # Fetch data for all three categories
    batting_df = fetch_espn_stats_by_category(SEASON_YEAR, 'batting')
    pitching_df = fetch_espn_stats_by_category(SEASON_YEAR, 'pitching')
    fielding_df = fetch_espn_stats_by_category(SEASON_YEAR, 'fielding')

    if batting_df is None or pitching_df is None or fielding_df is None:
        print("❌ Fatal Error: Failed to fetch one or more stat categories. Aborting.")
        sys.exit(1)
        
    # Merge the three dataframes into one
    print("  -> Merging all stat categories...")
    # Select key stats from each dataframe to avoid duplicate columns like 'gamesPlayed'
    batting_essentials = batting_df[['team_id', 'team_abbr', 'gamesPlayed', 'runs', 'hits', 'homeRuns', 'avg', 'strikeouts', 'walks']]
    pitching_essentials = pitching_df[['team_id', 'earnedRunAverage', 'walksAndHitsPerInningPitched']]
    fielding_essentials = fielding_df[['team_id', 'errors']]
    
    # Merge on the unique team ID
    merged_1 = pd.merge(batting_essentials, pitching_essentials, on='team_id', how='inner')
    final_df = pd.merge(merged_1, fielding_essentials, on='team_id', how='inner')

    # Rename columns to match our Supabase schema
    final_df = final_df.rename(columns={
        'avg': 'batting_average',
        'strikeouts': 'strikeouts_batting',
        'walks': 'walks_batting',
        'earnedRunAverage': 'era',
        'walksAndHitsPerInningPitched': 'whip'
    })

    # Sanitize data to be JSON compliant
    final_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    final_records = final_df.where(pd.notnull(final_df), None).to_dict('records')
    
    # Add the last_updated timestamp
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
        print(f"❌ Supabase upsert failed with an exception: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_and_upsert_team_stats()
