import requests
import sqlite3
import time

def get_sunrise_sunset(lat, lon):
    """Get sunrise and sunset times from the sunrise-sunset API."""
    url = "https://api.sunrise-sunset.org/json"
    params = { 
        "lat": lat,
        "lng": lon,
        "formatted": 0
    }
    response = requests.get(url, params=params)
    data = response.json()

    sunrise = data["results"]["sunrise"]
    sunset = data["results"]["sunset"]
    return sunrise, sunset

def store_sun_data(db_name="nba_project.db", batch_size=25):
    """Store sunrise and sunset info for all arenas in the database."""
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # Create table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS daylight_info (
            arena_id INTEGER PRIMARY KEY,
            sunrise TEXT,
            sunset TEXT
        )
    """)
    conn.commit()

    while True:
        # Pull a batch of arenas that haven't been processed yet
        cur.execute(f"""
            SELECT id, latitude, longitude
            FROM arenas
            WHERE id NOT IN (SELECT arena_id FROM daylight_info)
            LIMIT {batch_size}
        """)
        rows = cur.fetchall()
        if not rows:
            print("All arenas processed!")
            break

        for arena_id, lat, lon in rows:
            try:
                sunrise, sunset = get_sunrise_sunset(lat, lon)
                cur.execute("""
                    INSERT INTO daylight_info (arena_id, sunrise, sunset)
                    VALUES (?, ?, ?)
                """, (arena_id, sunrise, sunset))
                print(f"Stored data for arena_id {arena_id}")
                time.sleep(0.5)  # polite delay to avoid hammering the API
            except Exception as e:
                print(f"Error processing arena_id {arena_id}: {e}")

        conn.commit()

    conn.close()
    print("Finished storing sunrise/sunset data for all arenas.")

if __name__ == "__main__":
    store_sun_data()


conn = sqlite3.connect("nba_project.db")
cur = conn.cursor()

cur.execute("SELECT * FROM daylight_info LIMIT 10")
for row in cur.fetchall():
    print(row)

conn.close()
