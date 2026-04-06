import os
import json
import time
import logging
import pytz
from datetime import datetime
from playwright.sync_api import sync_playwright
from supabase import create_client, Client
from bs4 import BeautifulSoup
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

supabase: Client = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"]
)

_tz    = pytz.timezone('America/New_York')
_today = datetime.now(_tz)
TODAY  = _today.strftime('%Y-%m-%d')

# ── TEAM ABBR NORMALIZATION ────────────────────────────────────
# rotogrinders uses slightly different abbrevs in some cases
ABBR_MAP = {
    'WSH': 'WSN',
    'KC':  'KCR',
    'SD':  'SDP',
    'SF':  'SFG',
    'TB':  'TBR',
    'CWS': 'CHW',
}

def normalize_abbr(abbr):
    return ABBR_MAP.get(abbr.upper(), abbr.upper())

def parse_player_info(info_text):
    """
    Parse 'Nico Hoerner  (R) 2B $12.3K' -> (name='Nico Hoerner', hand='R', position='2B')
    Parse 'Jameson Taillon  (R) SP $15.9K' -> (name='Jameson Taillon', hand='R', position='SP')
    """
    if not info_text:
        return None, None, None
    info_text = info_text.strip()
    # Extract hand from parentheses: (R), (L), or (S)
    hand_match = re.search(r'\(([RLS])\)', info_text)
    hand = hand_match.group(1) if hand_match else None
    # Extract position: first word after (R)/(L)/(S), before the $ salary
    pos_match = re.search(r'\([RLS]\)\s+([A-Z0-9/]+)', info_text)
    position = pos_match.group(1) if pos_match else None
    # Name: everything before the opening parenthesis, stripped
    if '(' in info_text:
        name = info_text[:info_text.index('(')].strip()
    else:
        name = info_text.strip()
    return name, hand, position

# ── SCRAPE ────────────────────────────────────────────────────
def scrape_lineups():
    records = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36'
        )

        logging.info("📥 Loading rotogrinders.com/lineups/mlb ...")
        page.goto('https://rotogrinders.com/lineups/mlb', timeout=30000)

        # Wait for lineup cards to render
        page.wait_for_selector('.game-card', timeout=20000)
        time.sleep(2)  # let lazy content finish loading

        html = page.content()
        browser.close()

    logging.info("✅ Page loaded — parsing HTML")
    soup = BeautifulSoup(html, 'lxml')
    game_cards = soup.select('.game-card')
    logging.info(f"Found {len(game_cards)} game cards")

    for game in game_cards:
        # Team abbreviations from data-abbr
        team_els = game.select('[data-abbr]')
        team_abbrs = [normalize_abbr(el['data-abbr']) for el in team_els if el.get('data-abbr')]
        if len(team_abbrs) < 2:
            continue
        away_abbr = team_abbrs[0]
        home_abbr = team_abbrs[1]

        # Game time
        header = game.select_one('.module-header, .game-card-header')
        game_time = header.get_text(strip=True)[:20] if header else ''

        # Process each lineup card (away=first, home=second)
        lineup_cards = game.select('.lineup-card')
        for side_idx, lc in enumerate(lineup_cards[:2]):
            team = away_abbr if side_idx == 0 else home_abbr
            side = 'Away' if side_idx == 0 else 'Home'

            # Lineup status — check lineup-card-body class
            body = lc.select_one('.lineup-card-body')
            body_classes = body.get('class', []) if body else []
            if 'unconfirmed' in body_classes:
                status = 'Projected'   # "lineup not released" = projected
            else:
                status = 'Confirmed'

            # Starting pitcher
            pitcher_el = lc.select_one('.lineup-card-pitcher .player-nameplate-name')
            pitcher_info_el = lc.select_one('.lineup-card-pitcher .player-nameplate-info')
            pitcher_name = pitcher_el.get_text(strip=True) if pitcher_el else None
            pitcher_info = pitcher_info_el.get_text(strip=True) if pitcher_info_el else None
            _, pitcher_hand, _ = parse_player_info(pitcher_info)

            # Batting order
            player_rows = lc.select('.lineup-card-player')
            batting_order = []
            for i, row in enumerate(player_rows):
                name_el = row.select_one('.player-nameplate-name')
                info_el = row.select_one('.player-nameplate-info')
                info_text = info_el.get_text(strip=True) if info_el else ''
                full_name = name_el.get_text(strip=True) if name_el else None
                _, bat_hand, position = parse_player_info(info_text)
                if full_name:
                    batting_order.append({
                        'order':    i + 1,
                        'name':     full_name,
                        'position': position,
                        'bat_side': bat_hand,
                    })

            records.append({
                'team':          team,
                'side':          side,
                'game_date':     TODAY,
                'game_time':     game_time,
                'lineup_status': status,
                'pitcher_name':  pitcher_name,
                'pitcher_hand':  pitcher_hand,
                'batting_order': json.dumps(batting_order),
                'scrape_date':   _today.strftime('%Y-%m-%d %H:%M:%S %Z'),
            })
            logging.info(f"  {side} {team}: {len(batting_order)} batters, SP={pitcher_name}, status={status}")

    return records

# ── MAIN ──────────────────────────────────────────────────────
records = scrape_lineups()

if not records:
    logging.error("No records scraped — aborting")
    raise SystemExit(1)

logging.info(f"\n📊 Total records: {len(records)}")

# Delete today's rows and re-insert fresh
supabase.table("projected_lineups").delete().eq("game_date", TODAY).execute()
logging.info(f"🗑️  Cleared old rows for {TODAY}")

supabase.table("projected_lineups").upsert(
    records, on_conflict="team,game_date"
).execute()

logging.info(f"✅ {len(records)} rows upserted to projected_lineups")
