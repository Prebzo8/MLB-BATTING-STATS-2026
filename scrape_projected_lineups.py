import os
import json
import logging
import pytz
import unicodedata
from datetime import datetime
from supabase import create_client, Client
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

supabase: Client = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"]
)

_tz    = pytz.timezone('America/New_York')
_today = datetime.now(_tz)
TODAY  = _today.strftime('%Y-%m-%d')

ROTOGRINDERS_URL = "https://rotogrinders.com/lineups/mlb"

# RotoGrinders abbr -> standard abbr
RG_ABBR_MAP = {
    "WSH": "WSN",
    "CWS": "CHW",
    "KC":  "KCR",
    "SD":  "SDP",
    "SF":  "SFG",
    "TB":  "TBR",
    "TEX": "TEX",
    "OAK": "ATH",
    "ATH": "ATH",
    "SAC": "ATH",
}

def normalize_abbr(abbr):
    if not abbr:
        return abbr
    abbr = abbr.strip().upper()
    return RG_ABBR_MAP.get(abbr, abbr)

def normalize_name(name):
    """
    Strip accents, combining marks, and any non-ASCII characters.
    Keeps letters, digits, spaces, periods, hyphens, and apostrophes.
    e.g. 'Ronald Acuna Jr.' stays clean, Ohtani's name doesn't break anything.
    """
    if not name:
        return name
    # Strip accents/diacritics
    nfkd = unicodedata.normalize('NFKD', name)
    ascii_str = ''.join(c for c in nfkd if not unicodedata.combining(c))
    # Keep only safe characters: letters, digits, spaces, . - '
    cleaned = ''.join(c for c in ascii_str if c.isascii() and (c.isalnum() or c in " .'-"))
    return cleaned.strip()

def scrape_rotogrinders():
    """
    Launch headless Chromium, load RotoGrinders MLB lineups page,
    parse game cards and return list of team lineup dicts.
    """
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        )
        logging.info(f"Loading {ROTOGRINDERS_URL}")
        page.goto(ROTOGRINDERS_URL, wait_until="networkidle", timeout=60000)

        # Wait for at least one game card to appear
        try:
            page.wait_for_selector(".lineup-card", timeout=30000)
        except Exception:
            logging.warning("Timed out waiting for .lineup-card — page may have no games")

        html = page.content()
        browser.close()

    soup = BeautifulSoup(html, "lxml")
    cards = soup.select(".lineup-card")
    logging.info(f"Found {len(cards)} lineup cards")

    for card in cards:
        try:
            results.extend(parse_card(card))
        except Exception as e:
            logging.warning(f"Error parsing card: {e}")

    return results


def parse_card(card):
    """
    Parse a single RotoGrinders game card.
    Returns a list of 2 dicts (away team, home team).
    """
    # Game time
    time_el = card.select_one(".game-time, .time")
    game_time = time_el.get_text(strip=True) if time_el else ""

    # Team abbreviations — first is away, second is home
    team_els = card.select(".team-abbr, .abbr")
    if len(team_els) < 2:
        logging.warning("Could not find 2 team abbrs in card — skipping")
        return []

    away_abbr = normalize_abbr(team_els[0].get_text(strip=True))
    home_abbr = normalize_abbr(team_els[1].get_text(strip=True))

    # Lineup status — if card body has 'unconfirmed' class it's Projected
    card_body = card.select_one(".lineup-card-body, .card-body, .players")
    if card_body and "unconfirmed" in card_body.get("class", []):
        status = "Projected"
    else:
        status = "Confirmed"

    # Pitchers — first pitcher block is away, second is home
    pitcher_els = card.select(".pitcher, .starting-pitcher")
    away_pitcher_name, away_pitcher_hand = parse_pitcher(pitcher_els[0] if len(pitcher_els) > 0 else None)
    home_pitcher_name, home_pitcher_hand = parse_pitcher(pitcher_els[1] if len(pitcher_els) > 1 else None)

    # Batting orders — two columns of players
    player_cols = card.select(".players-col, .lineup-col, .batters-col")
    if len(player_cols) < 2:
        # Fallback: try splitting all player rows by team
        all_players = card.select(".player-row, .lineup-player, .batter")
        mid = len(all_players) // 2
        away_players = all_players[:mid]
        home_players = all_players[mid:]
    else:
        away_players = player_cols[0].select(".player-row, .lineup-player, .batter, li")
        home_players = player_cols[1].select(".player-row, .lineup-player, .batter, li")

    away_order = parse_batting_order(away_players)
    home_order = parse_batting_order(home_players)

    away_record = {
        "team":          away_abbr,
        "side":          "Away",
        "game_date":     TODAY,
        "game_time":     game_time,
        "lineup_status": status,
        "pitcher_name":  home_pitcher_name,   # away team faces the home pitcher
        "pitcher_hand":  home_pitcher_hand,
        "batting_order": json.dumps(away_order),
        "scrape_date":   _today.strftime("%Y-%m-%d %H:%M:%S %Z"),
    }

    home_record = {
        "team":          home_abbr,
        "side":          "Home",
        "game_date":     TODAY,
        "game_time":     game_time,
        "lineup_status": status,
        "pitcher_name":  away_pitcher_name,   # home team faces the away pitcher
        "pitcher_hand":  away_pitcher_hand,
        "batting_order": json.dumps(home_order),
        "scrape_date":   _today.strftime("%Y-%m-%d %H:%M:%S %Z"),
    }

    return [away_record, home_record]


def parse_pitcher(el):
    """Extract pitcher name and handedness from a pitcher element."""
    if el is None:
        return None, None
    name_el = el.select_one(".name, .pitcher-name, a")
    name = normalize_name(name_el.get_text(strip=True)) if name_el else normalize_name(el.get_text(strip=True))

    hand = None
    hand_el = el.select_one(".hand, .throw, .pitch-hand")
    if hand_el:
        raw = hand_el.get_text(strip=True).upper()
        if "R" in raw:
            hand = "R"
        elif "L" in raw:
            hand = "L"

    return name or None, hand


def parse_batting_order(player_els):
    """Parse a list of player elements into batting order dicts."""
    order = []
    for i, el in enumerate(player_els):
        name_el = el.select_one(".name, .player-name, a")
        name    = normalize_name(name_el.get_text(strip=True)) if name_el else normalize_name(el.get_text(strip=True))

        pos_el  = el.select_one(".position, .pos")
        pos     = pos_el.get_text(strip=True) if pos_el else ""

        hand_el = el.select_one(".hand, .bat-hand, .bats")
        bat_side = ""
        if hand_el:
            raw = hand_el.get_text(strip=True).upper()
            if raw in ("L", "R", "S"):
                bat_side = raw

        if name:
            order.append({
                "order":    i + 1,
                "name":     name,
                "position": pos,
                "bat_side": bat_side,
            })
    return order


def write_to_supabase(records):
    if not records:
        logging.info("No records to write")
        return

    # Delete old game dates
    supabase.table("projected_lineups").delete().lt("game_date", TODAY).execute()
    logging.info("Cleared old game dates")

    # Delete today's existing rows (full refresh)
    supabase.table("projected_lineups").delete().eq("game_date", TODAY).execute()
    logging.info(f"Cleared today's existing rows ({TODAY})")

    # Upsert fresh records
    supabase.table("projected_lineups").upsert(
        records,
        on_conflict="team,game_date"
    ).execute()
    logging.info(f"Upserted {len(records)} records to Supabase")


def main():
    # ── TIME-WINDOW GUARD ────────────────────────────────────────────────────
    # Only scrape projected lineups between 11:00 AM and 8:00 PM Eastern.
    # If the script is triggered outside this window (e.g. by a cron that runs
    # more broadly), it exits immediately without hitting RotoGrinders or
    # touching Supabase.
    now_et = datetime.now(_tz)
    window_start = now_et.replace(hour=11, minute=0,  second=0, microsecond=0)
    window_end   = now_et.replace(hour=20, minute=0,  second=0, microsecond=0)

    if not (window_start <= now_et <= window_end):
        logging.info(
            f"Outside scrape window (11 AM – 8 PM ET). "
            f"Current ET time: {now_et.strftime('%I:%M %p %Z')} — exiting."
        )
        return

    logging.info(
        f"Scraping RotoGrinders lineups for {TODAY} "
        f"(ET: {now_et.strftime('%I:%M %p %Z')})"
    )
    records = scrape_rotogrinders()
    logging.info(f"Parsed {len(records)} team lineup records")
    write_to_supabase(records)
    logging.info("Done")


if __name__ == "__main__":
    main()
