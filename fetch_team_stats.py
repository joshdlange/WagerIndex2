import os
import uuid
from dotenv import load_dotenv
from supabase import create_client, Client
from pybaseball import team_batting, team_pitching

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def generate_team_id():
    return str(uuid.uuid4())

def merge_team_stats(batting_df, pitching_df):
    stats = []
    for _, row in batting_df.iterrows():
        team = {
            "team_abbr": row["Team"],
            "team_name": row["Name"],
            "batting_avg": row["AVG"],
            "obp": row["OBP"],
            "slg": row["SLG"],
            "ops": row["OPS"],
            "runs": row["R"],
            "home_runs": row["HR"],
            "strikeouts": row["SO"],
            "walks": row["BB"],
        }
        stats.append(team)

    for stat in stats:
        pitching_row = pitching_df[pitching_df["Team"] == stat["team_abbr"]]
        if not pitching_row.empty:
            row = pitching_row.iloc[0]
            stat.update({
                "era": row["ERA"],
                "whip": row["WHIP"],
                "k_per_9": row["SO9"],
                "bb_per_9": row["BB9"],
                "innings_pitched": row["IP"],
            })
        stat["id"] = generate_team_id()
    return stats

def push_to_supabase(stats):
    print("📤 Previewing first 3 team stats:")
    print(stats[:3])
    response = supabase.table("team_stats").upsert(stats, on_conflict=["team_abbr"]).execute()
    print("✅ Supabase response:", response)

if __name__ == "__main__":
    batting_df = team_batting()
    pitching_df = team_pitching()
    stats = merge_team_stats(batting_df, pitching_df)
    push_to_supabase(stats)
