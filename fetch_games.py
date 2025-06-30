import os
import requests
import datetime
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_espn_games(target_date):
    date_str = target_date.strftime("%Y%m%d")
    url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={date_str}"
    response = requests.get(url)
    return response.json()


def parse_games(json_data):
    games = []
    for event in json_data.get("events", []):
        competitions = event.get("competitions", [])
        if not competitions:
            continue

        comp = competitions[0]
        competitors = comp.get("competitors", [])
        if len(competitors) != 2:
            continue

        home = next((team for team in competitors if team["homeAway"] == "home"), None)
        away = next((team for team in competitors if team["homeAway"] == "away"), None)

        if not home or not away:
            continue

        odds = comp.get("odds", [{}])[0]
        moneyline_home = int(odds.get("homeTeamOdds", {}).get("moneyLine", 0))
        moneyline_away = int(odds.get("awayTeamOdds", {}).get("moneyLine", 0))

        games.append({
            "game_date": datetime.datetime.fromisoformat(event["date"].replace("Z", "+00:00")).date().isoformat(),
            "home_team": home["team"]["displayName"],
            "away_team": away["team"]["displayName"],
            "home_team_abbr": home["team"]["abbreviation"],
            "away_team_abbr": away["team"]["abbreviation"],
            "espn_game_id": event.get("id"),
            "moneyline_home": moneyline_home,
            "moneyline_away": moneyline_away
        })

    return games


def push_to_supabase(games):
    supabase.table("games").upsert(games, on_conflict=["game_date", "home_team", "away_team"]).execute()


if __name__ == "__main__":
    target_date = datetime.datetime.today()
    print(f"🔄 Fetching ESPN game data for {target_date.date()}...")
    try:
        json_data = fetch_espn_games(target_date)
        games = parse_games(json_data)
        print(f"✅ Parsed {len(games)} games. Uploading to Supabase...")
        push_to_supabase(games)
        print("✅ Upload complete.")
    except Exception as e:
        print(f"❌ Error: {e}")
