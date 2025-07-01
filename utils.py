# utils.py
import requests
from datetime import datetime

def get_current_season_year():
    """
    Gets the correct, active MLB season year from the ESPN API.
    It intelligently handles the offseason, where the API points to the next year.
    """
    print(" Hitting ESPN API to get the official current season year...")
    try:
        url = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        season_data = data.get("season", {})
        api_year = season_data.get("year")
        season_type = season_data.get("type") # 1=preseason, 2=regular, 3=postseason, 4=offseason

        if not api_year or not season_type:
            raise ValueError("API response missing 'year' or 'type' in season object.")

        # THIS IS THE CRITICAL LOGIC
        if season_type == 4: # 4 indicates Offseason
            print(f"  -> API reports offseason for year {api_year}. Using previous year for stats.")
            return api_year - 1
        else:
            print(f"  -> API reports active season: {api_year}.")
            return api_year

    except Exception as e:
        print(f"  -> Critical failure fetching year from ESPN API ({e}). Aborting.")
        # We must exit here, as proceeding with the wrong year is catastrophic.
        raise
