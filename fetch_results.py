# fetch_results.py
import os
import sys
import requests
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}

def main():
    print("🚀 Starting Daily Results Check...")
    if not SUPABASE_URL or not SUPABASE_KEY: print("❌ Fatal Error: Supabase secrets must be set."); sys.exit(1)
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    print(f"  -> Fetching results for {yesterday}...")
    
    try:
        url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={yesterday}"
        response = requests.get(url, headers=HEADERS).json()
        
        results_to_upsert = []
        for event in response.get("events", []):
            if event.get("status", {}).get("type", {}).get("name") != "STATUS_FINAL":
                continue # Skip games that are not final

            comp = event["competitions"][0]
            home = next((c for c in comp["competitors"] if c["homeAway"] == "home"), {})
            away = next((c for c in comp["competitors"] if c["homeAway"] == "away"), {})

            winner_abbr = None
            if home.get('winner') is True:
                winner_abbr = home.get('team', {}).get('abbreviation')
            elif away.get('winner') is True:
                winner_abbr = away.get('team', {}).get('abbreviation')

            results_to_upsert.append({
                'game_id': event.get("id"),
                'home_score': int(home.get('score', 0)),
                'away_score': int(away.get('score', 0)),
                'actual_winner': winner_abbr
            })
        
        if not results_to_upsert:
            print("✅ No final game results found for yesterday.")
            return

        # Upsert the results back into the 'games' table
        print(f"⬆️ Upserting {len(results_to_upsert)} final game results...")
        response = supabase.table('games').upsert(results_to_upsert).execute()
        if not response.data: raise Exception(getattr(response, 'error', 'Unknown error'))
        print("✅ Game results updated successfully.")

    except Exception as e:
        print(f"❌ Fatal Error fetching results: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
