import os
from supabase import create_client
from pybaseball import team_batting, team_pitching

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_and_insert_team_stats():
    print("📊 Fetching team batting and pitching stats for current season...")

    batting_df = team_batting()
    pitching_df = team_pitching()

    print("🧮 Merging batting and pitching data...")
    merged_df = batting_df.merge(pitching_df, on="Team", suffixes=("_bat", "_pit"))

    print("⬆️ Upserting team stats into Supabase...")
    records = []
    for _, row in merged_df.iterrows():
        team_stats = {
            "team_name": row["Team"],
            "games_played": row.get("G_bat", 0),
            "runs": row.get("R", 0),
            "hits": row.get("H_bat", 0),
            "home_runs": row.get("HR_bat", 0),
            "batting_average": row.get("BA", 0),
            "strikeouts": row.get("SO_bat", 0),
            "walks": row.get("BB_bat", 0),
            "era": row.get("ERA", 0),
            "whip": row.get("WHIP", 0)
        }
        records.append(team_stats)

    # ✅ Use UPSERT – replaces INSERT
    response = supabase.table("team_stats").upsert(records, on_conflict=["team_name"]).execute()
    if hasattr(response, "error") and response.error:
        print("❌ Supabase Error:", response.error)
    else:
        print("✅ Team stats upserted successfully.")

if __name__ == "__main__":
    fetch_and_insert_team_stats()
