import requests
import sqlite3
import time

def get_sunrise_sunset(lat, lon, date):
    """Get sunrise and sunset times from the sunrise-sunset API for a specific date."""
    url = "https://api.sunrise-sunset.org/json"
    params = { 
        "lat": lat,
        "lng": lon,
        "date": date,  # Format: YYYY-MM-DD
        "formatted": 0,
        "tzid": "America/Detroit"
    }
    response = requests.get(url, params=params)
    data = response.json()

    if data["status"] == "OK":
        sunrise = data["results"]["sunrise"]
        sunset = data["results"]["sunset"]
        return sunrise, sunset
    else:
        raise Exception(f"API returned status: {data['status']}")

def store_sun_data(db_name="final_project_sportsdata.db", batch_size=25):
    """Store sunrise and sunset info for all games in the database."""
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # Create table for game-specific daylight info
    cur.execute("""
        CREATE TABLE IF NOT EXISTS game_daylight_info (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER UNIQUE,
            date_id INTEGER,
            home_team_id INTEGER,
            visitor_team_id INTEGER,
            arena_id INTEGER,
            home_latitude REAL,
            home_longitude REAL,
            sunrise TEXT,
            sunset TEXT,
            FOREIGN KEY (date_id) REFERENCES game_dates(date_id)
        )
    """)
    conn.commit()

    processed = 0
    
    while processed < batch_size:
        # Join games with arenas AND game_dates to get the actual date string
        cur.execute("""
            SELECT g.game_id, gd.date, g.date_id, g.home_team_id, g.visitor_team_id, a.id, a.latitude, a.longitude
            FROM games g
            JOIN arenas a ON g.home_team_id = a.id
            JOIN game_dates gd ON g.date_id = gd.date_id
            WHERE g.date_id IS NOT NULL
            AND g.game_id NOT IN (SELECT game_id FROM game_daylight_info WHERE game_id IS NOT NULL)
            LIMIT ?
        """, (batch_size - processed,))
        
        rows = cur.fetchall()
        
        if not rows:
            print("All available games processed!")
            break

        for game_id, game_date, date_id, home_team_id, visitor_team_id, arena_id, lat, lon in rows:
            try:
                # Check if location data exists
                if lat is None or lon is None:
                    print(f"Warning: No location data for arena_id {arena_id}, skipping game {game_id}")
                    continue
                
                sunrise, sunset = get_sunrise_sunset(lat, lon, game_date)
                
                cur.execute("""
                    INSERT INTO game_daylight_info 
                    (game_id, date_id, home_team_id, visitor_team_id, arena_id, home_latitude, home_longitude, sunrise, sunset)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (game_id, date_id, home_team_id, visitor_team_id, arena_id, lat, lon, sunrise, sunset))
                
                print(f"Stored daylight data for game_id {game_id} on {game_date} at arena {arena_id}")
                processed += 1
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                print(f"Error processing game_id {game_id}: {e}")

        conn.commit()

    conn.close()
    print(f"Finished storing sunrise/sunset data. Processed {processed} games.")

if __name__ == "__main__":
    store_sun_data()