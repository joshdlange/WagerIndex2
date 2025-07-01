# utils.py
import requests
from datetime import datetime

def get_current_season_year():
    print(" Hitting ESPN API to get the official current season year...")
    try:
        url = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard"
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        season_data = data.get("season", {})
        api_year, season_type = season_data.get("year"), season_data.get("type")

        if not api_year or not season_type:
            raise ValueError("API response missing 'year' or 'type'.")

        if season_type == 4: # 4 = Offseason
            print(f"  -> API reports offseason for year {api_year}. Using previous year ({api_year - 1}) for stats.")
            return api_year - 1
        else:
            print(f"  -> API reports active season: {api_year}.")
            return api_year
    except Exception as e:
        print(f"  -> CRITICAL FAILURE fetching year from ESPN API ({e}). Aborting.")
        raise
