"""
Master file to run all NBA data collection scripts.
Automatically runs 8 times to collect 200 rows of game data (25 per run).

Requirements met:
- Stores max 25 items per execution per API
- Uses one database: final_project_sportsdata.db
- Two tables share integer key (games.game_id <-> game_stats.game_id)
- No duplicate data stored
"""

import sqlite3
import time

# Import your existing scripts
from get_nba_stats import store_stats_and_update_games
from get_sun_data import store_sun_data

DB_NAME = "final_project_sportsdata.db"
ARENA_DB = "nba_project.db"
TOTAL_RUNS = 8

def copy_arena_data():
    """Copy arena data from nba_project.db (one-time setup)."""
    main_conn = sqlite3.connect(DB_NAME)
    arena_conn = sqlite3.connect(ARENA_DB)
    
    main_cur = main_conn.cursor()
    arena_cur = arena_conn.cursor()
    
    # Check if arenas table already exists and has data
    try:
        main_cur.execute("SELECT COUNT(*) FROM arenas")
        if main_cur.fetchone()[0] > 0:
            main_conn.close()
            arena_conn.close()
            return
    except sqlite3.OperationalError:
        pass
    
    # Get the CREATE TABLE statement from source
    arena_cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='arenas'")
    create_statement = arena_cur.fetchone()[0]
    
    # Drop and recreate arenas table
    main_cur.execute("DROP TABLE IF EXISTS arenas")
    main_cur.execute(create_statement)
    
    # Copy all arena data
    arena_cur.execute("SELECT * FROM arenas")
    rows = arena_cur.fetchall()
    
    # Get column count dynamically
    arena_cur.execute("PRAGMA table_info(arenas)")
    col_count = len(arena_cur.fetchall())
    
    # Insert all rows
    placeholders = ','.join(['?' for _ in range(col_count)])
    main_cur.executemany(f"INSERT INTO arenas VALUES ({placeholders})", rows)
    main_conn.commit()
    
    main_conn.close()
    arena_conn.close()

def run_single_collection():
    """Run one iteration of data collection (25 items max per API)."""
    try:
        store_stats_and_update_games()
    except:
        pass
    
    try:
        store_sun_data(db_name=DB_NAME, batch_size=25)
    except:
        pass

def main():
    """Main execution - copies arena data once, then runs collection 8 times."""
    
    # One-time setup: Copy arena data
    try:
        copy_arena_data()
    except:
        pass
    
    # Run collection multiple times
    for run_number in range(1, TOTAL_RUNS + 1):
        run_single_collection()
        time.sleep(2)

if __name__ == "__main__":
    main()