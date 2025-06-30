import os
import requests
import pandas as pd
from supabase import create_client

# --- Supabase setup ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Load team abbreviation to team_id mapping from Supabase ---
def get_team_ids():
    result = supabase.table("teams").select("id,abbreviation").execute()
    rows = result.data
    return {row["abbreviation"]: row["id"] for row in rows}

# --- Fetch pitcher stats from Sports Reference ---
def fetch_pitcher_stats():
    url = "https://www.baseball-reference.com/leagues/MLB/2024-standard-pitching.shtml"
    tables = pd.read_html(url)
    df = tables[0]

    # Clean column headers
    df.columns = df.columns.droplevel() if isinstance(df.columns, pd.MultiIndex) else df.columns

    df = df[df["Rk"] != "Rk"]  # Remove repeating headers
    df = df.dropna(subset=["Name"])

    return df

# --- Format and insert pitcher data ---
def insert_pitchers(df, team_ids):
    for _, row in df.iterrows():
        try:
            name = row["Name"]
            team_abbr = row["Tm"]

            if team_abbr not in team_ids or team_abbr == "TOT":
                continue

            pitcher = {
                "name": name,
                "team_id": team_ids[team_abbr],
                "era": float(row.get("ERA", 0) or 0),
                "whip": float(row.get("WHIP", 0) or 0),
                "k9": float(row.get("SO9", 0) or 0),
                "bb9": float(row.get("BB9", 0) or 0),
                "innings_pitched": float(row.get("IP", 0) or 0),
            }

            print(f"Inserting: {pitcher['name']} ({team_abbr})")
            supabase.table("pitchers").insert(pitcher).execute()

        except Exception as e:
            print(f"❌ Failed to insert {row.get('Name')}: {e}")

# --- Run ---
if __name__ == "__main__":
    print("🔄 Fetching team ID map...")
    team_ids = get_team_ids()

    print("📊 Fetching pitcher stats...")
    df = fetch_pitcher_stats()

    print("📥 Inserting into Supabase...")
    insert_pitchers(df, team_ids)

    print("✅ Pitcher sync complete.")
