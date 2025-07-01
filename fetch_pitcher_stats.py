# fetch_pitcher_stats.py
import os
from supabase import create_client, Client
from pybaseball import pitching_stats
from dotenv import load_dotenv
from datetime import datetime
import sys
import pandas as pd

load_dotenv()

# --- Config ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
CURRENT_YEAR = datetime.now().year
MIN_INNINGS_PITCHED = 10

def fetch_and_upsert_pitchers():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Fatal Error: SUPABASE_URL and SUPABASE_KEY secrets must be set.")
        sys.exit(1)

    print(f"📊 Fetching pitcher stats for the {CURRENT_YEAR} season using pybaseball...")
    try:
        pitchers_df = pitching_stats(CURRENT_YEAR)
        if pitchers_df.empty:
            print(f"✅ INFO: No pitcher stats data found for {CURRENT_YEAR}. This is expected before the season starts. Stopping job.")
            sys.exit(0)
    except Exception as e:
        print(f"❌ Error fetching data from pybaseball for {CURRENT_YEAR}: {e}")
        sys.exit(1)

    filtered_pitchers = pitchers_df[pitchers_df['IP'] >= MIN_INNINGS_PITCHED].copy()
    print(f"  - Found {len(filtered_pitchers)} pitchers with at least {MIN_INNINGS_PITCHED} IP.")

    if filtered_pitchers.empty:
        print(f"✅ INFO: No pitchers met the {MIN_INNINGS_PITCHED} IP minimum for {CURRENT_YEAR}. Stopping job.")
        sys.exit(0)

    records = filtered_pitchers.rename(columns={
        'Name': 'name', 'Team': 'team_abbr', 'ERA': 'era', 'WHIP': 'whip',
        'K/9': 'k9', 'BB/9': 'bb9', 'IP': 'innings_pitched'
    }).to_dict('records')

    # Add the last_updated timestamp
    for record in records:
        record['last_updated'] = datetime.now().isoformat()
        
    print(f"⬆️ Upserting {len(records)} pitchers into Supabase...")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        response = supabase.table("pitchers").upsert(records, on_conflict="name").execute()
        if response.data:
            print("✅ Pitcher stats upserted successfully.")
        else:
            print(f"❌ Supabase Error: {getattr(response, 'error', 'Unknown error')}")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Supabase upsert failed with an exception: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_and_upsert_pitchers()
