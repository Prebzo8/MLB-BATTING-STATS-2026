import os
import re
import sys
import json
import logging
import pytz
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

# ── ABBREVIATION MAP ──────────────────────────────────────────────────────────
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


# ── NAME DISPLAY MAP ──────────────────────────────────────────────────────────
# RotoGrinders omits accents and sometimes Jr./Sr.
# Map to canonical names that match fangraphs_player_splits exactly.
# Add entries here whenever "Not Found" appears in Lineup Stats.
DISPLAY_NAME_MAP = {
    "Ronald Acuna Jr.":       "Ronald Acuña Jr.",
    "Ronald Acuna":           "Ronald Acuña Jr.",
    "Fernando Tatis Jr.":     "Fernando Tatis Jr.",
    "Fernando Tatis":         "Fernando Tatis Jr.",
    "Bobby Witt Jr.":         "Bobby Witt Jr.",
    "Bobby Witt":             "Bobby Witt Jr.",
    "Vladimir Guerrero Jr.":  "Vladimir Guerrero Jr.",
    "Vladimir Guerrero":      "Vladimir Guerrero Jr.",
    "Jazz Chisholm Jr.":      "Jazz Chisholm Jr.",
    "Jazz Chisholm":          "Jazz Chisholm Jr.",
    "Luis Robert Jr.":        "Luis Robert Jr.",
    "Luis Robert":            "Luis Robert Jr.",
    "Luis Garcia Jr.":        "Luis García Jr.",
    "Luis Garcia":            "Luis García Jr.",
    "Jose Abreu":             "José Abreu",
    "Jose Ramirez":           "José Ramírez",
    "Yordan Alvarez":         "Yordan Álvarez",
    "Julio Rodriguez":        "Julio Rodríguez",
    "Eloy Jimenez":           "Eloy Jiménez",
}

def normalize_name(name):
    """Return canonical display name. Never strips Jr./Sr."""
    if not name:
        return name
    cleaned = ' '.join(name.strip().split())
    return DISPLAY_NAME_MAP.get(cleaned, cleaned)


# ── SELECTOR DISCOVERY ────────────────────────────────────────────────────────
# RotoGrinders is a React SPA — class names shift between deploys.
# We try multiple known patterns and use the first one that hits.

def find_game_cards(soup):
    candidates = [
        ".lineup-card",
        ".lineup__card",
        "[class*='lineup-card']",
        "[class*='lineupCard']",
        "[class*='game-card']",
        "[class*='gameCard']",
        "li.game",
        ".game",
    ]
    for sel in candidates:
        cards = soup.select(sel)
        if cards:
            logging.info(f"Game cards via selector '{sel}': {len(cards)}")
            return cards

    logging.warning("No game cards found — check rotogrinders_debug.html artifact")
    return []

def find_team_abbrs(card):
    candidates = [
        ".team-abbr", ".abbr", "[class*='team-abbr']", "[class*='teamAbbr']",
        "[class*='team__abbr']", "[class*='abbr']", ".tm",
    ]
    for sel in candidates:
        els = card.select(sel)
        if len(els) >= 2:
            return els[0].get_text(strip=True), els[1].get_text(strip=True)

    # Fallback: any leaf element with 2–3 uppercase letters
    abbr_re = re.compile(r'^[A-Z]{2,3}$')
    found = []
    for el in card.find_all(True):
        if el.find():
            continue
        t = el.get_text(strip=True)
        if abbr_re.match(t):
            found.append(t)
        if len(found) >= 2:
            return found[0], found[1]
    return None, None

def find_game_time(card):
    candidates = [
        ".game-time", ".time", "[class*='game-time']", "[class*='gameTime']",
        "[class*='time']", "time",
    ]
    for sel in candidates:
        el = card.select_one(sel)
        if el:
            t = el.get_text(strip=True)
            if re.search(r'\d', t) and (re.search(r'[AP]M', t, re.I) or ':' in t):
                return t
    return ""

def find_lineup_status(card):
    # Walk every element's classes. Check for 'unconfirmed'/'projected' FIRST
    # so they take priority if both appear (e.g. in CSS utility class names).
    # Default is Projected — never assume Confirmed.
    for el in card.find_all(True):
        classes = ' '.join(el.get('class', [])).lower()
        if 'unconfirmed' in classes or 'projected' in classes:
            return 'Projected'
        # Only treat 'confirmed' as a hit when it's a status class,
        # not part of a utility name like 'lineup-status-confirmed'
        if 'confirmed' in classes and 'status' not in classes:
            return 'Confirmed'
    return 'Projected'

def find_pitchers(card):
    candidates = [
        ".pitcher", ".starting-pitcher", "[class*='pitcher']",
        "[class*='Pitcher']", "[class*='sp']", "[class*='starter']",
    ]
    for sel in candidates:
        els = card.select(sel)
        if len(els) >= 2:
            return els[0], els[1]
        if len(els) == 1:
            return els[0], None
    return None, None

def find_player_cols(card):
    col_candidates = [
        ".players-col", ".lineup-col", ".batters-col",
        "[class*='players-col']", "[class*='lineupCol']",
        "[class*='lineup-col']", "[class*='lineup__players']",
    ]
    for sel in col_candidates:
        cols = card.select(sel)
        if len(cols) >= 2:
            row_sel = _find_row_sel(cols[0])
            away = cols[0].select(row_sel) if row_sel else []
            home = cols[1].select(row_sel) if row_sel else []
            if away or home:
                return away, home

    row_candidates = [
        ".player-row", ".lineup-player", ".batter",
        "[class*='player-row']", "[class*='lineupPlayer']",
        "[class*='batter']", "[class*='hitter']", "li",
    ]
    for sel in row_candidates:
        rows = card.select(sel)
        if len(rows) >= 2:
            mid = len(rows) // 2
            return rows[:mid], rows[mid:]
    return [], []

def _find_row_sel(col_el):
    for sel in [
        ".player-row", ".lineup-player", ".batter",
        "[class*='player-row']", "[class*='lineupPlayer']",
        "[class*='batter']", "li",
    ]:
        if col_el.select(sel):
            return sel
    return None


# ── ELEMENT PARSERS ───────────────────────────────────────────────────────────

def parse_pitcher(el):
    if el is None:
        return None, None
    name_el = el.select_one(".name, .pitcher-name, [class*='name'], [class*='Name'], a")
    raw  = name_el.get_text(strip=True) if name_el else el.get_text(strip=True)
    name = normalize_name(raw) or None

    hand_el     = el.select_one(".hand, .throw, .pitch-hand, [class*='hand'], [class*='Hand']")
    search_text = hand_el.get_text(strip=True) if hand_el else el.get_text(strip=True)
    m    = re.search(r'\b([RL])HP\b|\(([RL])\)', search_text.upper())
    hand = (m.group(1) or m.group(2)) if m else None
    return name, hand

def parse_batting_order(player_els):
    order = []
    for i, el in enumerate(player_els):
        name_el = el.select_one(".name, .player-name, [class*='name'], [class*='Name'], a")
        raw  = name_el.get_text(strip=True) if name_el else el.get_text(strip=True)
        name = normalize_name(raw)

        pos_el = el.select_one(".position, .pos, [class*='position'], [class*='pos']")
        pos    = pos_el.get_text(strip=True) if pos_el else ""
        if re.match(r'^\d+$', pos):
            pos = ""

        bat_side = ""
        hand_el  = el.select_one(".hand, .bat-hand, .bats, [class*='hand'], [class*='bats']")
        if hand_el:
            raw_h = hand_el.get_text(strip=True).upper()
            if raw_h in ("L", "R", "S"):
                bat_side = raw_h
        else:
            m = re.search(r'\(([LRS])\)', el.get_text())
            if m:
                bat_side = m.group(1)

        if name:
            order.append({
                "order":    i + 1,
                "name":     name,
                "position": pos,
                "bat_side": bat_side,
            })
    return order


# ── SCRAPER ───────────────────────────────────────────────────────────────────

def scrape_rotogrinders():
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page    = browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        )
        logging.info(f"Loading {ROTOGRINDERS_URL}")
        page.goto(ROTOGRINDERS_URL, wait_until="networkidle", timeout=60000)

        # Wait for any recognisable container — try each selector briefly
        for sel in [".lineup-card", "[class*='lineup']", "[class*='game-card']", ".game"]:
            try:
                page.wait_for_selector(sel, timeout=8000)
                logging.info(f"Page ready — matched '{sel}'")
                break
            except Exception:
                continue
        else:
            logging.warning("No selector matched — proceeding with whatever loaded")

        html = page.content()
        browser.close()

    # Always save debug HTML so you can inspect the real DOM via the
    # Actions → Artifacts → rotogrinders-debug-html download
    try:
        with open("rotogrinders_debug.html", "w", encoding="utf-8") as f:
            f.write(html)
        logging.info(f"Debug HTML saved ({len(html):,} chars)")
    except Exception as e:
        logging.warning(f"Could not save debug HTML: {e}")

    soup  = BeautifulSoup(html, "lxml")
    cards = find_game_cards(soup)
    logging.info(f"Parsing {len(cards)} cards")

    for card in cards:
        try:
            records = parse_card(card)
            if records:
                results.extend(records)
        except Exception as e:
            logging.warning(f"Card parse error: {e}", exc_info=True)

    return results


def parse_card(card):
    away_raw, home_raw = find_team_abbrs(card)
    if not away_raw or not home_raw:
        logging.warning("No team abbrs found — skipping card")
        return []

    away_abbr = normalize_abbr(away_raw)
    home_abbr = normalize_abbr(home_raw)
    game_time = find_game_time(card)
    status    = find_lineup_status(card)

    away_p_el, home_p_el        = find_pitchers(card)
    away_pitcher_name, away_pitcher_hand = parse_pitcher(away_p_el)
    home_pitcher_name, home_pitcher_hand = parse_pitcher(home_p_el)

    away_players, home_players = find_player_cols(card)
    away_order = parse_batting_order(away_players)
    home_order = parse_batting_order(home_players)

    logging.info(
        f"  {away_abbr} @ {home_abbr} | {game_time} | {status} | "
        f"away={len(away_order)} home={len(home_order)}"
    )

    return [
        {
            "team":          away_abbr,
            "side":          "Away",
            "game_date":     TODAY,
            "game_time":     game_time,
            "lineup_status": status,
            "pitcher_name":  home_pitcher_name,   # away faces home pitcher
            "pitcher_hand":  home_pitcher_hand,
            "batting_order": json.dumps(away_order),
            "scrape_date":   _today.strftime("%Y-%m-%d %H:%M:%S %Z"),
        },
        {
            "team":          home_abbr,
            "side":          "Home",
            "game_date":     TODAY,
            "game_time":     game_time,
            "lineup_status": status,
            "pitcher_name":  away_pitcher_name,   # home faces away pitcher
            "pitcher_hand":  away_pitcher_hand,
            "batting_order": json.dumps(home_order),
            "scrape_date":   _today.strftime("%Y-%m-%d %H:%M:%S %Z"),
        },
    ]


# ── SUPABASE ──────────────────────────────────────────────────────────────────

def write_to_supabase(records):
    if not records:
        logging.info("No records to write — nothing scraped")
        return

    # 1. Delete rows older than today
    supabase.table("projected_lineups") \
        .delete() \
        .lt("game_date", TODAY) \
        .execute()
    logging.info("Cleared rows older than today")

    # 2. Delete only Projected rows for today — leave Confirmed rows alone.
    #    scrape_confirmed_lineups.py owns Confirmed rows; we must not overwrite them.
    supabase.table("projected_lineups") \
        .delete() \
        .eq("game_date", TODAY) \
        .eq("lineup_status", "Projected") \
        .execute()
    logging.info(f"Cleared today's Projected rows ({TODAY})")

    # 3. Find which teams are already Confirmed so we skip them
    confirmed_res = supabase.table("projected_lineups") \
        .select("team") \
        .eq("game_date", TODAY) \
        .eq("lineup_status", "Confirmed") \
        .execute()
    confirmed_teams = {row["team"] for row in (confirmed_res.data or [])}

    to_insert = [r for r in records if r["team"] not in confirmed_teams]
    skipped   = len(records) - len(to_insert)

    if skipped:
        logging.info(f"Skipping {skipped} already-confirmed team(s): {confirmed_teams}")

    if to_insert:
        supabase.table("projected_lineups") \
            .upsert(to_insert, on_conflict="team,game_date") \
            .execute()
        logging.info(f"Upserted {len(to_insert)} projected records")
    else:
        logging.info("All teams already confirmed — nothing to upsert")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    now_et = datetime.now(_tz)

    # Allow bypassing the time window with: python scrape_projected_lineups.py --force
    # GitHub workflow_dispatch also passes --force so manual runs always execute.
    force = "--force" in sys.argv

    if not force:
        window_start = now_et.replace(hour=11, minute=0, second=0, microsecond=0)
        window_end   = now_et.replace(hour=20, minute=0, second=0, microsecond=0)
        if not (window_start <= now_et <= window_end):
            logging.info(
                f"Outside window (11 AM–8 PM ET). "
                f"Now: {now_et.strftime('%I:%M %p %Z')} — exiting. "
                f"Pass --force to override."
            )
            return

    logging.info(
        f"Scraping RotoGrinders for {TODAY} "
        f"(ET: {now_et.strftime('%I:%M %p %Z')})"
        + (" [FORCED]" if force else "")
    )
    records = scrape_rotogrinders()
    logging.info(f"Parsed {len(records)} records")
    write_to_supabase(records)
    logging.info("Done")


if __name__ == "__main__":
    main()
