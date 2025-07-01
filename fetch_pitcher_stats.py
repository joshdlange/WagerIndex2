# fetch_pitcher_stats.py
import os
import sys
import pandas as pd
import numpy as np
from supabase import create_client, Client
from pybaseball import pitching_stats
from dotenv import load_dotenv
from datetime import datetime
from utils import get_current_season_year # <-- IMPORT THE FIX

load_dotenv()

# --- Config ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
MIN_INNINGS_PITCHED = 10

def fetch_and_upsert_pitchers():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Fatal Error: SUPABASE_URL and SUPABASE_KEY secrets must be set.")
        sys.exit(1)

    SEASON_YEAR = get_current_season_year() # <-- USE THE FIX
    print(f"📊 Fetching pitcher stats for the {SEASON_YEAR} season using pybaseball...")

    try:
        pitchers_df = pitching_stats(SEASON_YEAR)
        if pitchers_df.empty:
            print(f"❌ Error: No pitcher stats data found for {SEASON_YEAR}. Aborting.")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Error fetching data from pybaseball for {SEASON_YEAR}: {e}")
        sys.exit(1)

    filtered_pitchers = pitchers_df[pitchers_df['IP'] >= MIN_INNINGS_PITCHED].copy()
    if filtered_pitchers.empty:
        print(f"✅ INFO: No pitchers met the {MIN_INNINGS_PITCHED} IP minimum. Stopping job.")
        sys.exit(0)

    # Sanitize data to be JSON compliant
    filtered_pitchers.replace([np.inf, -np.inf], np.nan, inplace=True)
    records = filtered_pitchers.where(pd.notnull(filtered_pitchers), None).to_dict('records')

    final_records = []
    for record in records:
        final_records.append({
            "name": record["Name"], "team_abbr": record["Team"],
            "era": record.get("ERA"), "whip": record.get("WHIP"),
            "k9": record.get("K/9"), "bb9": record.get("BB/9"),
            "innings_pitched": record.get("IP"), "last_updated": datetime.now().isoformat()
        })
        
    print(f"⬆️ Upserting {len(final_records)} pitchers into Supabase...")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        response = supabase.table("pitchers").upsert(final_records, on_conflict="name").execute()
        if response.data:
            print("✅ Pitcher stats upserted successfully.")
        else:
            print(f"❌ Supabase Error: {getattr(response, 'error', 'Unknown error')}")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Supabase upsert failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_and_upsert_pitchers()
