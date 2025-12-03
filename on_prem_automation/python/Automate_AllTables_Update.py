#!/usr/bin/env python
# coding: utf-8

# In[1]:


import logging
import os
import requests
import psycopg2
from datetime import datetime
import schedule
import time

# LOGGING CONFIG
log_file_path = os.path.join(os.getcwd(), "epl_etl_log.txt")

logger = logging.getLogger("EPL_ETL")
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

logger.info("Logging system initialized successfully.")
print("Logging initialized. Check your folder for epl_etl_log.txt")

# API CONFIG
API_KEY = "7215bf43b5de4fd2a9161700c44d5ee9"
BASE_URL = "https://api.football-data.org/v4"
HEADERS = {"X-Auth-Token": API_KEY}

# DB CONNECTION
def get_conn():
    return psycopg2.connect(
        host="localhost",
        user="postgres",
        password="bolanlelove",
        dbname="epl"
    )

# API FETCH FUNCTIONS 
def get_standings():
    url = f"{BASE_URL}/competitions/PL/standings"
    response = requests.get(url, headers=HEADERS)
    data = response.json()
    return data["standings"][0]["table"]

def get_scorers():
    url = f"{BASE_URL}/competitions/PL/scorers"
    response = requests.get(url, headers=HEADERS)
    data = response.json()
    return data["scorers"]

def get_fixtures():
    url = f"{BASE_URL}/competitions/PL/matches"
    response = requests.get(url, headers=HEADERS)
    data = response.json()
    return data["matches"]

# LOAD FUNCTIONS
def load_standings(rows):
    conn = get_conn()
    cur = conn.cursor()
    for team in rows:
        cur.execute("""
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
        """, (
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

def load_scorers(rows):
    conn = get_conn()
    cur = conn.cursor()
    for s in rows:
        cur.execute("""
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
        """, (
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

def load_fixtures(rows):
    conn = get_conn()
    cur = conn.cursor()
    for m in rows:
        home_score = m["score"]["fullTime"]["home"]
        away_score = m["score"]["fullTime"]["away"]
        cur.execute("""
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
        """, (
            m["id"],
            m["utcDate"],
            m["homeTeam"]["name"],
            m["awayTeam"]["name"],
            m["status"],
            home_score,
            away_score,
            datetime.now()
        ))
    conn.commit()
    cur.close()
    conn.close()

# MASTER ETL FUNCTION 
def run_etl():
    try:
        logger.info("Scheduled ETL started")
        standings = get_standings()
        scorers = get_scorers()
        fixtures = get_fixtures()
        load_standings(standings)
        load_scorers(scorers)
        load_fixtures(fixtures)
        logger.info("SUCCESS — All EPL tables updated")
        print(f"ETL completed successfully at {datetime.now()}")
    except Exception as e:
        logger.error(f"ERROR — {str(e)}")
        print("An error occurred. Check log file.")

# SCHEDULER
schedule.every().day.at("08:00").do(run_etl)
schedule.every().day.at("20:00").do(run_etl)

print("Scheduler initialized. Script will run at 08:00 and 20:00 daily.")

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(60)

