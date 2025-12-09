import requests
import sqlite3

with open("SportsAPIKey.txt", "r") as f:
    API_KEY = f.read().strip()


base_url = "https://api.balldontlie.io/v1/stats"

def fetch_stats(cursor=None):
    params = {
        "player_ids[]": [15, 46, 53, 57, 73, 89, 101, 130, 133, 250, 251, 290, 324, 367, 375, 450],
        "seasons[]": [2022, 2023],
        "per_page": 25
    }
    if cursor:
        params["cursor"] = cursor 

    headers = {"Authorization": API_KEY} 

    r = requests.get(base_url, params=params, headers=headers)
    if r.status_code != 200:
        print(f"Error fetching stats: {r.status_code}")
        return {"data": [], "meta": {"next_cursor": None}}

    return r.json()

def store_players(cur, data):
    for item in data["data"]:
        p = item["player"]
        cur.execute("SELECT player_id FROM players WHERE player_id=?", (p["id"],))
        if cur.fetchone():
            continue

        cur.execute("""
            INSERT INTO players (player_id, first_name, last_name, position, team_id)
            VALUES (?, ?, ?, ?, ?)
        """, (
            p["id"],
            p["first_name"],
            p["last_name"],
            p["position"],
            item["team"]["id"]
        ))

def store_games(cur, data):
    for item in data["data"]:
        game_id = item["game"]["id"]

        cur.execute("SELECT game_id FROM games WHERE game_id=?", (game_id,))
        if cur.fetchone():
            continue   

        cur.execute("INSERT INTO games (game_id) VALUES (?)", (game_id,))

def store_stats_and_update_games():
    conn = sqlite3.connect("final_project_sportsdata.db")
    cur = conn.cursor()

    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS players (
            player_id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            position TEXT,
            team_id INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS games (
            game_id INTEGER PRIMARY KEY,
            date TEXT,
            time TEXT,
            home_team_id INTEGER,
            visitor_team_id INTEGER,
            season INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS game_stats (
            stat_id INTEGER PRIMARY KEY,
            player_id INTEGER,
            game_id INTEGER,
            pts INTEGER,
            reb INTEGER,
            ast INTEGER
        )
    """)

    inserted_stats = 0
    cursor = None

    while inserted_stats < 25:
        data = fetch_stats(cursor)

        
        store_players(cur, data)
        store_games(cur, data)

        
        for item in data["data"]:
            stat_id = item["id"]

            cur.execute("SELECT stat_id FROM game_stats WHERE stat_id=?", (stat_id,))
            if cur.fetchone():
                continue

            cur.execute("""
                INSERT INTO game_stats (stat_id, player_id, game_id, pts, reb, ast)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                stat_id,
                item["player"]["id"],
                item["game"]["id"],
                item["pts"],
                item["reb"],
                item["ast"]
            ))

            inserted_stats += 1
            if inserted_stats >= 25:
                break

        cursor = data["meta"].get("next_cursor")
        if not cursor:
            break

    conn.commit()

    print(f"Inserted {inserted_stats} new stats rows")

    # ------------------- UPDATE GAME DETAILS -------------------
    inserted_games = 0
    cur.execute("SELECT game_id FROM games WHERE date IS NULL OR time IS NULL")
    game_ids = [row[0] for row in cur.fetchall()]

    for game_id in game_ids:
        if inserted_games >= 25:
            break

        r = requests.get(f"https://api.balldontlie.io/v1/games/{game_id}", headers={"Authorization": API_KEY})
        if r.status_code != 200:
            continue

        game = r.json()
        iso_timestamp = game["data"]["datetime"]  # e.g., "2022-10-18T00:00:00.000Z"
        date_part = iso_timestamp.split("T")[0]
        time_part = iso_timestamp.split("T")[1].replace("Z", "")
        
        if time_part.startswith("00:00"):
            time_part = None

        cur.execute("""
            UPDATE games
            SET date = ?, time = ?, home_team_id = ?, visitor_team_id = ?, season = ?
            WHERE game_id = ?
        """, (
            date_part,
            time_part,
            game["data"]["home_team"]["id"],
            game["data"]["visitor_team"]["id"],
            game["data"]["season"],
            game_id
        ))

        inserted_games += 1

    conn.commit()
    conn.close()
    print(f"Updated {inserted_games} games with date/time")

if __name__ == "__main__":
    store_stats_and_update_games()