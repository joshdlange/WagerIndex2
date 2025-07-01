# utils.py
import requests

def get_current_season_year():
    """
    Gets the current MLB season year directly from the ESPN API.
    This is the single source of truth for the season year.
    """
    print(" Hitting ESPN API to get the official current season year...")
    try:
        url = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        # The 'season' object contains the correct year
        year = data.get("season", {}).get("year")
        if year:
            print(f"  -> Official season year is {year}.")
            return year
        # Fallback in case the API structure changes
        print("  -> Could not find season year in API, falling back to system clock.")
        from datetime import datetime
        return datetime.now().year
    except Exception as e:
        print(f"  -> Failed to get year from ESPN API ({e}), falling back to system clock.")
        from datetime import datetime
        return datetime.now().year
