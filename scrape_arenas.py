"""
scrape_arenas.py

Author: Luke Burden
Description:
    Scrape NBA arena information from Wikipedia and store it in a database.

Function:
    get_arena_coordinates(url, db_name="nba_project.db")

    - Input:
        url (str): Wikipedia URL for "List of NBA arenas"
        db_name (str): SQLite database filename (default: nba_project.db)

    - Output:
        List of dictionaries with keys:
            "arena_name", "team", "city", "latitude", "longitude"

    - Side effect:
        Creates/updates an "arenas" table in the given SQLite database.
"""

import re
import requests
from bs4 import BeautifulSoup
import sqlite3
import time
from urllib.parse import urljoin


def create_arenas_table(conn):
    """
    Create the arenas table if it does not already exist.

    Schema:
        arenas(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            arena_name TEXT,
            team TEXT,
            city TEXT,
            latitude REAL,
            longitude REAL
        )
    """
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS arenas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            arena_name TEXT UNIQUE,
            team TEXT,
            city TEXT,
            latitude REAL,
            longitude REAL
        );
        """
    )
    conn.commit()


def parse_coordinates(cell):
    """
    Try to extract latitude and longitude from a table cell (the location cell).

    Handles several Wikipedia patterns:
      - <span class="geo-dec">decimal°N decimal°W</span>
      - <span class="geo">lat; lon</span>
      - DMS (degrees/minutes/seconds) with directional letters

    Returns:
        (lat, lon) as floats, or (None, None) if not found.
    """
    def token_to_decimal(token: str):
        """Parse a token that may be decimal or DMS and return a signed float."""
        token = token.strip()
        # normalize unicode minus
        token = token.replace("−", "-")

        # detect direction
        dir_match = re.search(r"([NSEW])", token, re.I)
        direction = dir_match.group(1).upper() if dir_match else None

        # extract all numbers (degrees, minutes, seconds or a single decimal)
        nums = re.findall(r"(\d+(?:\.\d+)?)", token)
        if not nums:
            return None

        try:
            if len(nums) >= 3:
                deg = float(nums[0])
                minutes = float(nums[1])
                seconds = float(nums[2])
                val = deg + minutes / 60.0 + seconds / 3600.0
            else:
                # one or two numbers: treat first as decimal degrees
                val = float(nums[0])
        except ValueError:
            return None

        if direction in ("S", "W"):
            val = -abs(val)
        # if token contains an explicit negative sign, honor it
        if token.strip().startswith("-"):
            val = -abs(val)

        return val

    # 1) geo-dec span (e.g. "40.7506°N 73.9935°W")
    geo_dec = cell.find("span", class_="geo-dec") if cell else None
    if geo_dec is not None:
        text = geo_dec.get_text(" ", strip=True)
        # look for tokens that include directional letters or decimal degrees
        # split by whitespace and try to parse two tokens
        tokens = text.split()
        vals = []
        for t in tokens:
            dec = token_to_decimal(t)
            if dec is not None:
                vals.append(dec)
            if len(vals) == 2:
                break
        if len(vals) >= 2:
            return vals[0], vals[1]

    # 2) geo span (often "lat; lon" or "lat, lon")
    geo_span = cell.find("span", class_="geo") if cell else None
    if geo_span is not None:
        text = geo_span.get_text(strip=True)
        # split on semicolon or comma
        if ";" in text:
            parts = [p.strip() for p in text.split(";")]
        elif "," in text:
            parts = [p.strip() for p in text.split(",")]
        else:
            parts = text.split()

        if len(parts) >= 2:
            try:
                lat = float(parts[0])
                lon = float(parts[1])
                return lat, lon
            except ValueError:
                # try token parsing for each part (handles DMS inside)
                lat = token_to_decimal(parts[0])
                lon = token_to_decimal(parts[1])
                if lat is not None and lon is not None:
                    return lat, lon

    # 3) fallback: try to parse the visible text of the cell
    if cell is not None:
        text = cell.get_text(" ", strip=True)
        # first try to find decimal numbers with directional letters
        dec_dir_matches = re.findall(r"([+-]?\d+(?:\.\d+)?)\s*°?\s*([NSEW])", text, flags=re.I)
        if len(dec_dir_matches) >= 2:
            vals = []
            for num, d in dec_dir_matches[:2]:
                v = float(num)
                if d.upper() in ("S", "W"):
                    v = -abs(v)
                vals.append(v)
            return vals[0], vals[1]

        # otherwise, look for two standalone decimals
        nums = re.findall(r"([+-]?\d+\.\d+|[+-]?\d+)", text)
        if len(nums) >= 2:
            try:
                lat = float(nums[0])
                lon = float(nums[1])
                return lat, lon
            except ValueError:
                pass

    return None, None


def get_arena_coordinates(url: str, db_name: str = "nba_project.db"):
    """
    Scrape arena name, team, city, and coordinates from the given Wikipedia URL
    and store them in the arenas table in the specified SQLite database.

    Parameters:
        url (str): Wikipedia URL for the NBA arenas page
        db_name (str): SQLite database file name

    Returns:
        List[dict]: list of rows like
            {
                "arena_name": ...,
                "team": ...,
                "city": ...,
                "latitude": ...,
                "longitude": ...
            }
    """
    # ---- Fetch page ----
    # Use a browser-like User-Agent to avoid simple blocks by Wikipedia.
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/115.0.0.0 Safari/537.36"
        )
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to fetch URL {url}: {e}")
    
    def fetch_soup(page_url: str):
        """Fetch a URL and return its BeautifulSoup object with the same headers."""
        try:
            resp = requests.get(page_url, headers=headers, timeout=15)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException:
            return None
    soup = BeautifulSoup(response.text, "html.parser")

    # ---- Find the correct table ----
    # We look for a table whose header contains "Arena" and "Team".
    candidate_tables = soup.find_all("table", class_="wikitable")
    target_table = None

    for table in candidate_tables:
        header = table.find("tr")
        if not header:
            continue
        table_header_names = [th.get_text(strip=True) for th in header.find_all("th")]
        header_text = " ".join(table_header_names).lower()

        if "arena" in header_text and "team" in header_text:
            target_table = table
            break

    if target_table is None:
        raise RuntimeError("Could not find NBA arenas table on the page.")

    # ---- Identify column indices ----
    header_cells = target_table.find("tr").find_all("th")
    header_names = [th.get_text(strip=True) for th in header_cells]

    # Try to match column names robustly
    arena_idx = None
    team_idx = None
    location_idx = None

    for i, h in enumerate(header_names):
        h_low = h.lower()
        if "arena" in h_low and arena_idx is None:
            arena_idx = i
        if "team" in h_low and team_idx is None:
            team_idx = i
        # location column may be called 'Location', 'City', or include 'city'
        if ("location" in h_low or "city" in h_low) and location_idx is None:
            location_idx = i

    if arena_idx is None or team_idx is None or location_idx is None:
        raise RuntimeError("Could not find Arena/Team/Location columns in the table header.")

    # ---- Process all rows ----
    arenas_data = []
    rows = target_table.find_all("tr")[1:]  # skip header

    for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) <= max(arena_idx, team_idx, location_idx):
            continue

        arena_name = cells[arena_idx].get_text(strip=True)
        team = cells[team_idx].get_text(strip=True)
        location_cell = cells[location_idx]
        location_text = location_cell.get_text(" ", strip=True)

        # City is usually before the first comma
        city = location_text.split(",")[0].strip()

        lat, lon = parse_coordinates(location_cell)

        # If missing coordinates, try to follow the arena link to its page
        if (lat is None or lon is None) and cells[arena_idx].find("a") is not None:
            a_tag = cells[arena_idx].find("a")
            href = a_tag.get("href")
            if href:
                arena_url = urljoin(url, href)
                # polite delay to avoid hammering Wikipedia
                time.sleep(0.5)
                arena_soup = fetch_soup(arena_url)
                if arena_soup:
                    # Coordinates on article pages are often in a span.geo or span.geo-dec
                    coord_container = arena_soup.find("span", class_="geo") or arena_soup.find("span", class_="geo-dec")
                    if coord_container:
                        # wrap in a temporary tag so parse_coordinates can operate
                        from bs4 import Tag

                        temp = Tag(name="div")
                        temp.append(coord_container)
                        lat2, lon2 = parse_coordinates(temp)
                        if lat2 is not None and lon2 is not None:
                            lat, lon = lat2, lon2

        # Skip empty rows
        if not arena_name:
            continue

        arenas_data.append(
            {
                "arena_name": arena_name,
                "team": team,
                "city": city,
                "latitude": lat,
                "longitude": lon,
            }
        )

    # ---- Store in database ----
    conn = sqlite3.connect(db_name)
    create_arenas_table(conn)
    cur = conn.cursor()

    def ensure_arena_name_unique(conn):
        """If `arena_name` isn't protected by a UNIQUE constraint, migrate the table to add it."""
        c = conn.cursor()
        # If table doesn't exist or already has UNIQUE index on arena_name, do nothing
        c.execute("PRAGMA table_info('arenas')")
        cols = c.fetchall()
        if not cols:
            return

        c.execute("PRAGMA index_list('arenas')")
        indexes = c.fetchall()
        for idx in indexes:
            # idx = (seq, name, unique, origin, partial)
            name = idx[1]
            unique = idx[2]
            if unique:
                c.execute(f"PRAGMA index_info('{name}')")
                info = c.fetchall()
                if any(i[2] == 'arena_name' for i in info):
                    return

        # Need to migrate: rename, create new table with UNIQUE, copy data, drop old
        c.execute("ALTER TABLE arenas RENAME TO arenas_old;")
        c.execute(
            """
            CREATE TABLE arenas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                arena_name TEXT UNIQUE,
                team TEXT,
                city TEXT,
                latitude REAL,
                longitude REAL
            );
            """
        )
        # Copy distinct rows (avoid duplicates)
        c.execute(
            "INSERT OR IGNORE INTO arenas (arena_name,team,city,latitude,longitude) SELECT arena_name,team,city,latitude,longitude FROM arenas_old;"
        )
        c.execute("DROP TABLE arenas_old;")
        conn.commit()

    ensure_arena_name_unique(conn)

    # No pre-delete: use UPSERT to insert or update rows

    for arena in arenas_data:
        cur.execute(
            """
            INSERT INTO arenas (arena_name, team, city, latitude, longitude)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(arena_name) DO UPDATE SET
                team=excluded.team,
                city=excluded.city,
                latitude=excluded.latitude,
                longitude=excluded.longitude;
            """,
            (
                arena["arena_name"],
                arena["team"],
                arena["city"],
                arena["latitude"],
                arena["longitude"],
            ),
        )

    conn.commit()
    # Export to CSV for easy sharing
    try:
        import csv

        cur.execute("SELECT arena_name,team,city,latitude,longitude FROM arenas ORDER BY id;")
        rows = cur.fetchall()
        with open("arenas.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["arena_name", "team", "city", "latitude", "longitude"])
            writer.writerows(rows)
    except Exception:
        pass

    conn.close()

    return arenas_data


if __name__ == "__main__":
    WIKI_URL = "https://en.wikipedia.org/wiki/List_of_NBA_arenas"
    data = get_arena_coordinates(WIKI_URL)
    # Print only the formatted pipe-separated rows, numbered 1..N
    for idx, row in enumerate(data, start=1):
        print(f"{idx}|{row['arena_name']}|{row['team']}|{row['city']}|{row['latitude']}|{row['longitude']}")
