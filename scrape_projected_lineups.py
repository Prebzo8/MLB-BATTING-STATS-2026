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

_tz = pytz.timezone('America/New_York')

ROTOGRINDERS_URL = "https://rotogrinders.com/lineups/mlb"

# ── ABBREVIATION MAP ──────────────────────────────────────────────────────────
RG_ABBR_MAP = {
    "WAS": "WSN",
    "WSH": "WSN",
    "CWS": "CHW",
    "KC":  "KCR",
    "SD":  "SDP",
    "SF":  "SFG",
    "TB":  "TBR",
    "OAK": "ATH",
    "SAC": "ATH",
}

def normalize_abbr(abbr):
    if not abbr:
        return abbr
    abbr = abbr.strip().upper()
    return RG_ABBR_MAP.get(abbr, abbr)


# ── NAME DISPLAY MAP ──────────────────────────────────────────────────────────
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
    if not name:
        return name
    cleaned = ' '.join(name.strip().split())
    return DISPLAY_NAME_MAP.get(cleaned, cleaned)


# ── PARSERS ───────────────────────────────────────────────────────────────────
# DOM structure (confirmed from rotogrinders_debug.html):
#
#   div.module.game-card                      ← one per matchup
#     div.game-card-header
#       div.game-card-weather
#         span.small                          ← "6:35 PM ET"
#       div.game-card-teams
#         span.team-nameplate-title[data-abbr="ARI"]  (×2)
#     div.game-card-body
#       div.game-card-lineups
#         div.lineup-card  (×2: [0]=away, [1]=home)
#           div.lineup-card-header
#             div.lineup-card-pitcher         ← has class "break" when projected
#               a.player-nameplate-name       ← pitcher name
#               span.player-nameplate-stats span.small  ← "(R)"
#           div.lineup-card-body
#             ul.lineup-card-players
#               li.lineup-card-player  (×9)
#                 span[data-position]         ← position
#                 a.player-nameplate-name     ← batter name
#                 span.player-nameplate-stats span.small  ← "(L)"

def parse_game_card(gc, today):
    """Parse one div.module.game-card into away + home records."""

    # Team abbrs
    team_els = gc.select(".team-nameplate-title[data-abbr]")
    if len(team_els) < 2:
        logging.warning("Could not find 2 team abbrs — skipping card")
        return []
    away_abbr = normalize_abbr(team_els[0].get("data-abbr", ""))
    home_abbr = normalize_abbr(team_els[1].get("data-abbr", ""))

    # Game time
    time_el   = gc.select_one(".game-card-weather .small")
    game_time = time_el.get_text(strip=True) if time_el else ""

    # Lineup cards: [0] = away, [1] = home
    lineup_cards = gc.select(".lineup-card")
    if len(lineup_cards) < 2:
        logging.warning(f"{away_abbr} @ {home_abbr} — fewer than 2 lineup-card elements, skipping")
        return []
    lc_away = lineup_cards[0]
    lc_home = lineup_cards[1]

    # Lineup status — RotoGrinders adds "break" class to pitcher div when projected
    # We always write "Projected" here; the confirmed scraper is the authoritative
    # source for flipping a row to "Confirmed"
    def get_status(lc):
        pitcher_div = lc.select_one(".lineup-card-pitcher")
        if pitcher_div and "break" in pitcher_div.get("class", []):
            return "Projected"
        return "Projected"   # default to Projected — confirmed scraper owns Confirmed

    away_status = get_status(lc_away)
    home_status = get_status(lc_home)
    status = away_status  # both sides get same status

    # Pitchers — each lineup-card shows that team's own starting pitcher
    def parse_pitcher(lc):
        pitcher_el = lc.select_one(".lineup-card-pitcher .player-nameplate-name")
        name = normalize_name(pitcher_el.get_text(strip=True)) if pitcher_el else None
        hand = None
        spans = lc.select(".lineup-card-pitcher .player-nameplate-stats span.small")
        if spans:
            m = re.search(r'\(([RL])\)', spans[0].get_text(strip=True))
            if m:
                hand = m.group(1)
        return name, hand

    away_pitcher_name, away_pitcher_hand = parse_pitcher(lc_away)
    home_pitcher_name, home_pitcher_hand = parse_pitcher(lc_home)

    # Batting orders
    def parse_batting_order(lc):
        order = []
        for i, player_el in enumerate(lc.select(".lineup-card-player")):
            name_el = player_el.select_one(".player-nameplate-name")
            name    = normalize_name(name_el.get_text(strip=True)) if name_el else ""
            if not name:
                continue
            nameplate = player_el.select_one("[data-position]")
            pos       = nameplate.get("data-position", "") if nameplate else ""
            bat_side  = ""
            spans = player_el.select(".player-nameplate-stats span.small")
            if spans:
                m = re.search(r'\(([LRS])\)', spans[0].get_text(strip=True))
                if m:
                    bat_side = m.group(1)
            order.append({
                "order":    i + 1,
                "name":     name,
                "position": pos,
                "bat_side": bat_side,
            })
        return order

    away_order = parse_batting_order(lc_away)
    home_order = parse_batting_order(lc_home)

    logging.info(
        f"  {away_abbr} @ {home_abbr} | {game_time} | {status} | "
        f"away={len(away_order)} home={len(home_order)}"
    )

    scrape_ts = datetime.now(_tz).strftime("%Y-%m-%d %H:%M:%S %Z")

    return [
        {
            "team":          away_abbr,
            "side":          "Away",
            "game_date":     today,
            "game_time":     game_time,
            "lineup_status": status,
            "pitcher_name":  away_pitcher_name,   # own starting pitcher
            "pitcher_hand":  away_pitcher_hand,
            "batting_order": json.dumps(away_order),
            "scrape_date":   scrape_ts,
        },
        {
            "team":          home_abbr,
            "side":          "Home",
            "game_date":     today,
            "game_time":     game_time,
            "lineup_status": status,
            "pitcher_name":  home_pitcher_name,   # own starting pitcher
            "pitcher_hand":  home_pitcher_hand,
            "batting_order": json.dumps(home_order),
            "scrape_date":   scrape_ts,
        },
    ]


# ── SCRAPER ───────────────────────────────────────────────────────────────────

def scrape_rotogrinders(today):
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

        try:
            page.wait_for_selector(".module.game-card", timeout=20000)
            logging.info("Page ready — .module.game-card found")
        except Exception:
            logging.warning("Timed out waiting for .module.game-card — proceeding anyway")

        html = page.content()
        browser.close()

    try:
        with open("rotogrinders_debug.html", "w", encoding="utf-8") as f:
            f.write(html)
        logging.info(f"Debug HTML saved ({len(html):,} chars)")
    except Exception as e:
        logging.warning(f"Could not save debug HTML: {e}")

    soup       = BeautifulSoup(html, "lxml")
    game_cards = soup.select(".module.game-card")
    logging.info(f"Found {len(game_cards)} game cards")

    results = []
    for gc in game_cards:
        try:
            records = parse_game_card(gc, today)
            results.extend(records)
        except Exception as e:
            logging.warning(f"Game card parse error: {e}", exc_info=True)

    return results


# ── SUPABASE ──────────────────────────────────────────────────────────────────

def write_to_supabase(records, today):
    if not records:
        logging.info("No records to write")
        return

    # Delete rows older than today
    supabase.table("projected_lineups") \
        .delete().lt("game_date", today).execute()
    logging.info("Cleared rows older than today")

    # Delete only Projected rows for today — leave Confirmed rows untouched
    supabase.table("projected_lineups") \
        .delete().eq("game_date", today).eq("lineup_status", "Projected").execute()
    logging.info(f"Cleared today's Projected rows ({today})")

    # Skip teams already confirmed
    confirmed_res = supabase.table("projected_lineups") \
        .select("team").eq("game_date", today).eq("lineup_status", "Confirmed").execute()
    confirmed_teams = {row["team"] for row in (confirmed_res.data or [])}

    to_insert = [r for r in records if r["team"] not in confirmed_teams]
    skipped   = len(records) - len(to_insert)
    if skipped:
        logging.info(f"Skipping {skipped} already-confirmed team(s): {confirmed_teams}")

    if not to_insert:
        logging.info("All teams already confirmed — nothing to upsert")
        return

    # Deduplicate by (team, game_date) — prevents ON CONFLICT crash
    seen = {}
    for r in to_insert:
        seen[(r["team"], r["game_date"])] = r
    deduped = list(seen.values())
    if len(deduped) < len(to_insert):
        logging.warning(f"Removed {len(to_insert) - len(deduped)} duplicate records")

    supabase.table("projected_lineups") \
        .upsert(deduped, on_conflict="team,game_date").execute()
    logging.info(f"Upserted {len(deduped)} projected records")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    # Compute today inside main() so it's always the correct date at run time
    now_et = datetime.now(_tz)
    today  = now_et.strftime('%Y-%m-%d')
    force  = "--force" in sys.argv

    if not force:
        window_start = now_et.replace(hour=11, minute=0, second=0, microsecond=0)
        window_end   = now_et.replace(hour=21, minute=0, second=0, microsecond=0)
        if not (window_start <= now_et <= window_end):
            logging.info(
                f"Outside window (11 AM–9 PM ET). "
                f"Now: {now_et.strftime('%I:%M %p %Z')} — exiting. "
                f"Pass --force to override."
            )
            return

    logging.info(
        f"Scraping RotoGrinders for {today} "
        f"(ET: {now_et.strftime('%I:%M %p %Z')})"
        + (" [FORCED]" if force else "")
    )
    records = scrape_rotogrinders(today)
    logging.info(f"Parsed {len(records)} records")
    write_to_supabase(records, today)
    logging.info("Done")


if __name__ == "__main__":
    main()
