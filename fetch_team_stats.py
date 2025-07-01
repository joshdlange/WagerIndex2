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
# Headers to mimic a real browser visit
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}

def scrape_espn_table(url):
    """
    Directly scrapes the HTML tables from a given ESPN stats URL.
    This function is designed to handle ESPN's format of splitting
    stats into two tables (American League and National League).
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        # pd.read_html returns a list of all tables on the page
        tables = pd.read_html(response.text)
        # ESPN stats pages have two main tables for AL and NL. We combine them.
        if len(tables) < 2:
            raise ValueError("Expected at least two tables on the page, found fewer.")
        df = pd.concat([tables[0], tables[1]], ignore_index=True)
        return df
    except Exception as e:
        print(f"❌ Failed to scrape or parse URL {url}: {e}")
        return None

def fetch_and_upsert_team_stats():
    """
    Fetches team stats by scraping the three primary ESPN stats pages
    (batting, pitching, fielding) and merging the results.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Fatal Error: SUPABASE_URL and SUPABASE_KEY secrets must be set.")
        sys.exit(1)

    try:
        SEASON_YEAR = get_current_season_year()
    except Exception:
        sys.exit(1)

    # --- URLs for the actual web pages ---
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

    # --- Data Cleaning and Merging ---
    print("  -> Cleaning and merging scraped data...")
    # The team name is in the first column, let's rename it for clarity
    batting_df = batting_df.rename(columns={batting_df.columns[0]: 'TeamName'})
    pitching_df = pitching_df.rename(columns={pitching_df.columns[0]: 'TeamName'})
    fielding_df = fielding_df.rename(columns={fielding_df.columns[0]: 'TeamName'})
    
    # Extract just the team name (e.g., "Los Angeles Angels" from "Los Angeles AngelsLAA")
    batting_df['TeamName'] = batting_df['TeamName'].str.extract(r'([a-zA-Z\s.]+)')[0].str.strip()

    # Select essential columns
    batting_essentials = batting_df[['TeamName', 'GP', 'R', 'H', 'HR', 'AVG', 'SO', 'BB']]
    pitching_essentials = pitching_df[['TeamName', 'ERA', 'WHIP']]
    fielding_essentials = fielding_df[['TeamName', 'E']]

    # Merge the dataframes
    merged_1 = pd.merge(batting_essentials, pitching_essentials, on='TeamName', how='inner')
    final_df = pd.merge(merged_1, fielding_essentials, on='TeamName', how='inner')

    # Now, we need to map the full team name to the abbreviation for our Supabase schema
    # We can create this map from the successful fetch_games.py script or define it. For now, let's create a temp one.
    # A more robust solution would be to pull this mapping from a 'teams' table in Supabase.
    team_abbr_map = {team_info['team']['displayName']: team_info['team']['abbreviation'] for team_info in requests.get("https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams").json()['sports'][0]['leagues'][0]['teams']}
    final_df['team_abbr'] = final_df['TeamName'].map(team_abbr_map)

    # Drop rows where we couldn't map an abbreviation
    final_df.dropna(subset=['team_abbr'], inplace=True)
    
    # Rename columns to match Supabase
    final_df = final_df.rename(columns={
        'GP': 'games_played', 'R': 'runs', 'H': 'hits', 'HR': 'home_runs', 'AVG': 'batting_average',
        'SO': 'strikeouts_batting', 'BB': 'walks_batting', 'E': 'errors'
    })

    # Sanitize data
    final_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    final_records = final_df.where(pd.notnull(final_df), None).to_dict('records')
    for record in final_records:
        record['last_updated'] = datetime.now().isoformat()

    print(f"⬆️ Upserting {len(final_records)} teams' complete stats into Supabase...")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        # We upsert on the abbreviation, which is our unique key
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
