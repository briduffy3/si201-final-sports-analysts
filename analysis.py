"""
Analysis: Player performance in games before vs after sunset
Compares player stats (points, rebounds, assists) for games that start before sunset vs after sunset.
"""

import sqlite3
from datetime import datetime, timedelta

DB_NAME = "final_project_sportsdata.db"

def parse_time(time_str):
    """Parse ISO time string to datetime object."""
    if not time_str:
        return None
    try:
        # Handle ISO format with timezone
        return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
    except:
        return None

def analyze_player_performance():
    """Analyze player performance before vs after sunset."""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    # Get all game stats with sunset times
    cur.execute("""
        SELECT 
            gs.player_id,
            p.first_name,
            p.last_name,
            gs.pts,
            gs.reb,
            gs.ast,
            g.date,
            g.time,
            gd.sunset
        FROM game_stats gs
        JOIN players p ON gs.player_id = p.player_id
        JOIN games g ON gs.game_id = g.game_id
        JOIN game_daylight_info gd ON g.game_id = gd.game_id
        WHERE g.time IS NOT NULL AND gd.sunset IS NOT NULL
    """)
    
    rows = cur.fetchall()
    
    # Dictionary to store player stats
    player_stats = {}
    
    for row in rows:
        player_id, first_name, last_name, pts, reb, ast, game_date, game_time, sunset_time = row
        
        # Parse sunset time (already in local timezone)
        sunset_dt = parse_time(sunset_time)
        
        # Parse game time (in UTC) and convert to local time
        if not sunset_dt:
            continue
        
        try:
            # Combine date and time, assume UTC
            game_datetime_str = f"{game_date}T{game_time}"
            game_dt_utc = datetime.fromisoformat(game_datetime_str.replace('Z', '+00:00'))
            
            # Convert UTC to local timezone using sunset's offset
            if sunset_dt.utcoffset():
                game_dt_local = game_dt_utc + sunset_dt.utcoffset()
            else:
                # Default to EST (-5 hours)
                game_dt_local = game_dt_utc - timedelta(hours=5)
            
        except:
            continue
        
        # Determine if game is before or after sunset
        is_before_sunset = game_dt_local.time() < sunset_dt.time()
        
        # Initialize player entry if not exists
        if player_id not in player_stats:
            player_stats[player_id] = {
                'name': f"{first_name} {last_name}",
                'before_sunset': {'pts': [], 'reb': [], 'ast': [], 'games': 0},
                'after_sunset': {'pts': [], 'reb': [], 'ast': [], 'games': 0}
            }
        
        # Add stats to appropriate category
        category = 'before_sunset' if is_before_sunset else 'after_sunset'
        player_stats[player_id][category]['pts'].append(pts or 0)
        player_stats[player_id][category]['reb'].append(reb or 0)
        player_stats[player_id][category]['ast'].append(ast or 0)
        player_stats[player_id][category]['games'] += 1
    
    # Calculate averages
    results = {}
    for player_id, stats in player_stats.items():
        before = stats['before_sunset']
        after = stats['after_sunset']
        
        # Only include players with games in both categories
        if before['games'] > 0 and after['games'] > 0:
            results[player_id] = {
                'name': stats['name'],
                'before_sunset': {
                    'games': before['games'],
                    'avg_pts': sum(before['pts']) / before['games'],
                    'avg_reb': sum(before['reb']) / before['games'],
                    'avg_ast': sum(before['ast']) / before['games']
                },
                'after_sunset': {
                    'games': after['games'],
                    'avg_pts': sum(after['pts']) / after['games'],
                    'avg_reb': sum(after['reb']) / after['games'],
                    'avg_ast': sum(after['ast']) / after['games']
                },
                'differences': {
                    'pts_diff': (sum(after['pts']) / after['games']) - (sum(before['pts']) / before['games']),
                    'reb_diff': (sum(after['reb']) / after['games']) - (sum(before['reb']) / before['games']),
                    'ast_diff': (sum(after['ast']) / after['games']) - (sum(before['ast']) / before['games'])
                }
            }
    
    conn.close()
    return results

def write_analysis_to_file():
    """Write analysis results to a text file."""
    results = analyze_player_performance()
    
    with open('sunset_analysis_results.txt', 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("PLAYER PERFORMANCE ANALYSIS: BEFORE vs AFTER SUNSET\n")
        f.write("=" * 80 + "\n\n")
        
        if not results:
            f.write("No data available for analysis.\n")
            return
        
        # Sort by biggest point difference
        sorted_players = sorted(results.items(), key=lambda x: abs(x[1]['differences']['pts_diff']), reverse=True)
        
        for player_id, data in sorted_players:
            f.write(f"{data['name']} (ID: {player_id})\n")
            f.write("-" * 80 + "\n")
            
            before = data['before_sunset']
            after = data['after_sunset']
            diff = data['differences']
            
            f.write(f"BEFORE SUNSET ({before['games']} games):\n")
            f.write(f"  Points: {before['avg_pts']:.2f} | Rebounds: {before['avg_reb']:.2f} | Assists: {before['avg_ast']:.2f}\n")
            
            f.write(f"AFTER SUNSET ({after['games']} games):\n")
            f.write(f"  Points: {after['avg_pts']:.2f} | Rebounds: {after['avg_reb']:.2f} | Assists: {after['avg_ast']:.2f}\n")
            
            f.write(f"DIFFERENCE:\n")
            f.write(f"  Points: {diff['pts_diff']:+.2f} | Rebounds: {diff['reb_diff']:+.2f} | Assists: {diff['ast_diff']:+.2f}\n")
            
            # Interpretation
            if abs(diff['pts_diff']) > 2:
                direction = "better" if diff['pts_diff'] > 0 else "worse"
                f.write(f"  >> Performs {direction} after sunset ({abs(diff['pts_diff']):.2f} pts difference)\n")
            
            f.write("\n")

if __name__ == "__main__":
    write_analysis_to_file()