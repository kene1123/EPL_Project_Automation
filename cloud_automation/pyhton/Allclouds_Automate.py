import logging
import os
import requests
import psycopg2
from datetime import datetime
import schedule
import time

# -------------------------
# LOGGING
# -------------------------
log_file_path = os.path.join(os.getcwd(), "epl_cloud_etl_log.txt")

logger = logging.getLogger("CLOUD_EPL_ETL")
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s — %(levelname)s — %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

logger.info("Cloud ETL logger initialized successfully")
print("Cloud ETL logging initialized.")

# -------------------------
# API CONFIG
# -------------------------
API_KEY = "7215bf43b5de4fd2a9161700c44d5ee9"
BASE_URL = "https://api.football-data.org/v4"
HEADERS = {"X-Auth-Token": API_KEY}

# -------------------------
# CLOUD DB CONNECTION
# -------------------------
def get_cloud_conn():
    return psycopg2.connect(
        host="ep-nameless-river-agukj3qw-pooler.c-2.eu-central-1.aws.neon.tech",
        user="neondb_owner",
        password="npg_1dWkloHw3UKp",
        dbname="epl_db",
        sslmode="require"
    )

# -------------------------
# API FETCH FUNCTIONS
# -------------------------
def get_standings():
    r = requests.get(f"{BASE_URL}/competitions/PL/standings", headers=HEADERS)
    r.raise_for_status()
    return r.json()["standings"][0]["table"]

def get_scorers():
    r = requests.get(f"{BASE_URL}/competitions/PL/scorers", headers=HEADERS)
    r.raise_for_status()
    return r.json()["scorers"]

def get_fixtures():
    r = requests.get(f"{BASE_URL}/competitions/PL/matches", headers=HEADERS)
    r.raise_for_status()
    return r.json()["matches"]

# -------------------------
# LOAD FUNCTIONS (same as your original)
# -------------------------
def load_standings(rows):
    sql = """
        INSERT INTO standings (
            team_id, team_name, position, played_games, won, draw, lost,
            points, goals_for, goals_against, goal_difference, last_update
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (team_id) DO UPDATE SET
            team_name = EXCLUDED.team_name,
            position = EXCLUDED.position,
            played_games = EXCLUDED.played_games,
            won = EXCLUDED.won,
            draw = EXCLUDED.draw,
            lost = EXCLUDED.lost,
            points = EXCLUDED.points,
            goals_for = EXCLUDED.goals_for,
            goals_against = EXCLUDED.goals_against,
            goal_difference = EXCLUDED.goal_difference,
            last_update = EXCLUDED.last_update;
    """
    try:
        conn = get_cloud_conn()
        cur = conn.cursor()
        for team in rows:
            cur.execute(sql, (
                team["team"]["id"],
                team["team"]["name"],
                team["position"],
                team["playedGames"],
                team["won"],
                team["draw"],
                team["lost"],
                team["points"],
                team["goalsFor"],
                team["goalsAgainst"],
                team["goalDifference"],
                datetime.now()
            ))
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Standings updated successfully: {len(rows)} teams")
    except Exception as e:
        logger.error(f"Standings update failed: {e}")

def load_scorers(rows):
    sql = """
        INSERT INTO scorers (
            player_id, player_name, team_name, goals, assists, last_update
        )
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (player_id) DO UPDATE SET
            player_name = EXCLUDED.player_name,
            team_name = EXCLUDED.team_name,
            goals = EXCLUDED.goals,
            assists = EXCLUDED.assists,
            last_update = EXCLUDED.last_update;
    """
    try:
        conn = get_cloud_conn()
        cur = conn.cursor()
        for s in rows:
            cur.execute(sql, (
                s["player"]["id"],
                s["player"]["name"],
                s["team"]["name"],
                s["goals"],
                s.get("assists", 0),
                datetime.now()
            ))
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Scorers updated successfully: {len(rows)} players")
    except Exception as e:
        logger.error(f"Scorers update failed: {e}")

def load_fixtures(rows):
    sql = """
        INSERT INTO fixtures (
            match_id, utc_date, home_team, away_team, status,
            home_score, away_score, last_update
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (match_id) DO UPDATE SET
            utc_date = EXCLUDED.utc_date,
            home_team = EXCLUDED.home_team,
            away_team = EXCLUDED.away_team,
            status = EXCLUDED.status,
            home_score = EXCLUDED.home_score,
            away_score = EXCLUDED.away_score,
            last_update = EXCLUDED.last_update;
    """
    try:
        conn = get_cloud_conn()
        cur = conn.cursor()
        for m in rows:
            cur.execute(sql, (
                m["id"],
                m["utcDate"],
                m["homeTeam"]["name"],
                m["awayTeam"]["name"],
                m["status"],
                m["score"]["fullTime"]["home"],
                m["score"]["fullTime"]["away"],
                datetime.now()
            ))
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Fixtures updated successfully: {len(rows)} matches")
    except Exception as e:
        logger.error(f"Fixtures update failed: {e}")

# -------------------------
# MASTER ETL FUNCTION
# -------------------------
def run_etl():
    try:
        logger.info("Scheduled Cloud ETL started")
        standings = get_standings()
        scorers = get_scorers()
        fixtures = get_fixtures()
        load_standings(standings)
        load_scorers(scorers)
        load_fixtures(fixtures)
        logger.info("SUCCESS — Cloud DB updated")
        print(f"Cloud ETL completed successfully at {datetime.now()}")
    except Exception as e:
        logger.error(f"ERROR — {str(e)}")
        print("Cloud ETL failed. Check log.")

# -------------------------
# SCHEDULER
# -------------------------
schedule.every().day.at("08:00").do(run_etl)
schedule.every().day.at("20:00").do(run_etl)

print("Cloud scheduler initialized. Script will run at 08:00 and 20:00 daily.")

# Keep script running
while True:
    schedule.run_pending()
    time.sleep(60)
