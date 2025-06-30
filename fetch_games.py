import os
import uuid
import datetime
import requests
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables (ensure these are set in GitHub Secrets)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_espn_games(target_date):
    date_str = target_date.strftime("%Y%m%d")
    url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={date_str}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data.get("events", [])

def extract_game_info(event):
    try:
        competitions = event["competitions"][0]
        competitors = competitions["competitors"]
        home = next(team for team in competitors if team["homeAway"] == "home")
        away = next(team for team in competitors if team["homeAway"] == "away")

        odds = competitions.get("odds", [{}])[0]
        moneyline_home = odds.get("homeTeamOdds", {}).get("moneyLine")
        moneyline_away = odds.get("awayTeamOdds", {}).get("moneyLine")

        game_info = {
            "id": str(uuid.uuid4()),
            "game_date": datetime.datetime.strptime(event["date"], "%Y-%m-%dT%H:%MZ").date(),
            "home_team": home["team"]["displayName"],
            "away_team": away["team"]["displayName"],
            "home_team_abbr": home["team"]["abbreviation"],
            "away_team_abbr": away["team"]["abbreviation"],
            "moneyline_home": moneyline_home,
            "moneyline_away": moneyline_away,
            "espn_game_id": event["id"],
            "created_at": datetime.datetime.utcnow().isoformat()
        }
        return game_info
    except Exception as e:
        print(f"❌ Error extracting game info: {e}")
        return None

def push_to_supabase(games):
    try:
        supabase.table("games").upsert(games, on_conflict=["game_date", "home_team", "away_team"]).execute()
        print(f"✅ Uploaded {len(games)} games to Supabase.")
    except Exception as e:
        print(f"❌ Supabase upload failed: {e}")

def main():
    today = datetime.date.today()
    print(f"📅 Fetching games for {today}")
    events = fetch_espn_games(today)

    games = []
    for event in events:
        game = extract_game_info(event)
        if game:
            games.append(game)

    if games:
        push_to_supabase(games)
    else:
        print("⚠️ No games found to upload.")

if __name__ == "__main__":
    main()
