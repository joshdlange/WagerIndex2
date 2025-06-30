import os
import datetime
import requests
from supabase import create_client, Client
from dotenv import load_dotenv

# Load env vars
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_espn_results(target_date):
    date_str = target_date.strftime("%Y%m%d")
    url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={date_str}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get("events", [])

def extract_results(events):
    results = []
    for event in events:
        try:
            comp = event["competitions"][0]
            competitors = comp["competitors"]
            home = next(team for team in competitors if team["homeAway"] == "home")
            away = next(team for team in competitors if team["homeAway"] == "away")

            home_score = int(home["score"])
            away_score = int(away["score"])
            winner = home["team"]["displayName"] if home["winner"] else away["team"]["displayName"]

            game = {
                "game_date": datetime.datetime.strptime(event["date"], "%Y-%m-%dT%H:%MZ").date(),
                "home_team": home["team"]["displayName"],
                "away_team": away["team"]["displayName"],
                "home_score": home_score,
                "away_score": away_score,
                "winner": winner,
                "espn_game_id": event["id"]
            }
            results.append(game)
        except Exception as e:
            print(f"⚠️ Error parsing game: {e}")
    return results

def push_to_supabase(results):
    try:
        supabase.table("games").upsert(results, on_conflict=["game_date", "home_team", "away_team"]).execute()
        print(f"✅ Uploaded {len(results)} final scores to Supabase.")
    except Exception as e:
        print(f"❌ Supabase upload failed: {e}")

def main():
    today = datetime.date.today()
    print(f"📊 Fetching MLB results for {today}")
    events = fetch_espn_results(today)
    parsed = extract_results(events)
    if parsed:
        push_to_supabase(parsed)
    else:
        print("⚠️ No results found.")

if __name__ == "__main__":
    main()
