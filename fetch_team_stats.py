import os
import uuid
from datetime import date
import pandas as pd
import requests
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_and_insert_team_stats():
    print("📊 Fetching team batting and pitching stats for current season...")

    # Fetch batting data
    batting_url = "https://statsapi.mlb.com/api/v1/stats?stats=season&group=hitting&season=2025&sportId=1"
    batting_resp = requests.get(batting_url).json()
    batting_rows = batting_resp["stats"][0]["splits"]

    # Fetch pitching data
    pitching_url = "https://statsapi.mlb.com/api/v1/stats?stats=season&group=pitching&season=2025&sportId=1"
    pitching_resp = requests.get(pitching_url).json()
    pitching_rows = pitching_resp["stats"][0]["splits"]

    print("🔄 Merging batting and pitching data...")

    batting_data = []
    for row in batting_rows:
        team = row["team"]["name"]
        stats = row["stat"]
        batting_data.append({
            "team_name": team,
            "runs": float(stats.get("runs", 0)),
            "games_played": float(stats.get("gamesPlayed", 1)),  # avoid div by 0
        })

    pitching_data = []
    for row in pitching_rows:
        team = row["team"]["name"]
        stats = row["stat"]
        pitching_data.append({
            "team_name": team,
            "era": float(stats.get("era", 0)),
        })

    batting_df = pd.DataFrame(batting_data)
    batting_df["runs_per_game"] = batting_df["runs"] / batting_df["games_played"]
    batting_df.drop(columns=["runs", "games_played"], inplace=True)

    pitching_df = pd.DataFrame(pitching_data)

    merged_df = pd.merge(batting_df, pitching_df, on="team_name", how="inner")

    print("📥 Inserting team stats into Supabase...")

    for _, row in merged_df.iterrows():
        try:
            supabase.table("team_stats").upsert({
                "id": str(uuid.uuid4()),
                "team_name": row["team_name"],
                "era": round(row["era"], 2) if pd.notna(row["era"]) else None,
                "runs_per_game": round(row["runs_per_game"], 2) if pd.notna(row["runs_per_game"]) else None,
                "updated_at": date.today().isoformat()
            }).execute()
            print(f"✅ Inserted {row['team_name']}")
        except Exception as e:
            print(f"❌ Error inserting {row['team_name']}: {e}")

if __name__ == "__main__":
    fetch_and_insert_team_stats()
