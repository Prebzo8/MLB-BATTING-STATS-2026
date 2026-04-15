import os
import json
import logging
import pytz
import requests
from datetime import datetime
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

supabase: Client = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"]
)

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

_tz = pytz.timezone('America/New_York')

MLB_API = "https://statsapi.mlb.com/api/v1"

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


# ── TEAM MAP ──────────────────────────────────────────────────────────────────
MLB_NAME_TO_ABBR = {
    "Arizona Diamondbacks":    "ARI",
    "Atlanta Braves":          "ATL",
    "Baltimore Orioles":       "BAL",
    "Boston Red Sox":          "BOS",
    "Chicago Cubs":            "CHC",
    "Chicago White Sox":       "CHW",
    "Cincinnati Reds":         "CIN",
    "Cleveland Guardians":     "CLE",
    "Colorado Rockies":        "COL",
    "Detroit Tigers":          "DET",
    "Houston Astros":          "HOU",
    "Kansas City Royals":      "KCR",
    "Los Angeles Angels":      "LAA",
    "Los Angeles Dodgers":     "LAD",
    "Miami Marlins":           "MIA",
    "Milwaukee Brewers":       "MIL",
    "Minnesota Twins":         "MIN",
    "New York Mets":           "NYM",
    "New York Yankees":        "NYY",
    "Oakland Athletics":       "ATH",
    "Sacramento Athletics":    "ATH",
    "Athletics":               "ATH",
    "Philadelphia Phillies":   "PHI",
    "Pittsburgh Pirates":      "PIT",
    "San Diego Padres":        "SDP",
    "San Francisco Giants":    "SFG",
    "Seattle Mariners":        "SEA",
    "St. Louis Cardinals":     "STL",
    "Tampa Bay Rays":          "TBR",
    "Texas Rangers":           "TEX",
    "Toronto Blue Jays":       "TOR",
    "Washington Nationals":    "WSN",
}


# ── TELEGRAM ──────────────────────────────────────────────────────────────────
def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.info("Telegram not configured — skipping")
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=10
        )
        r.raise_for_status()
    except Exception as e:
        logging.warning(f"Telegram send failed: {e}")


# ── MLB API ───────────────────────────────────────────────────────────────────
def get_todays_games(today):
    url = f"{MLB_API}/schedule?sportId=1&date={today}&hydrate=team"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logging.error(f"Schedule fetch failed: {e}")
        return []

    games = []
    for date_block in data.get("dates", []):
        for g in date_block.get("games", []):
            away_name = g["teams"]["away"]["team"]["name"]
            home_name = g["teams"]["home"]["team"]["name"]
            away_abbr = MLB_NAME_TO_ABBR.get(away_name)
            home_abbr = MLB_NAME_TO_ABBR.get(home_name)
            if not away_abbr or not home_abbr:
                logging.warning(f"Unknown team: '{away_name}' or '{home_name}' — skipping")
                continue

            game_time_raw = g.get("gameDate", "")
            try:
                gt_utc    = datetime.strptime(game_time_raw, "%Y-%m-%dT%H:%M:%SZ")
                gt_utc    = pytz.utc.localize(gt_utc)
                gt_et     = gt_utc.astimezone(_tz)
                game_time = gt_et.strftime("%-I:%M %p ET")
            except Exception:
                game_time = ""

            games.append({
                "gamePk":   g["gamePk"],
                "awayAbbr": away_abbr,
                "homeAbbr": home_abbr,
                "awayName": away_name,
                "homeName": home_name,
                "gameTime": game_time,
            })

    logging.info(f"Found {len(games)} games today ({today})")
    return games


def get_lineup(game_pk):
    """
    Fetch confirmed lineup from MLB Stats API.
    Returns dict with away/home data, or None if neither side is posted yet.
    Handles partial lineups — if only one side is confirmed, returns that side.
    """
    url = f"{MLB_API}/game/{game_pk}/lineups"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logging.warning(f"gamePk {game_pk} lineup fetch error: {e}")
        return None

    home_batters = data.get("homeBatters", [])
    away_batters = data.get("awayBatters", [])

    # If neither side has batters, lineup not posted at all
    if not home_batters and not away_batters:
        return None

    def parse_batters(batters):
        order = []
        for i, p in enumerate(batters):
            pos = (
                p.get("primaryPosition", {}).get("abbreviation")
                or p.get("primaryPosition", {}).get("code", "")
            )
            order.append({
                "order":    i + 1,
                "name":     normalize_name(p.get("fullName", "")),
                "position": pos,
                "bat_side": p.get("batSide", {}).get("code", ""),
            })
        return order

    def parse_pitcher(pitcher_list):
        if not pitcher_list:
            return None, None
        p = pitcher_list[0]
        return (
            normalize_name(p.get("fullName", "")),
            p.get("pitchHand", {}).get("code", ""),
        )

    away_pitcher_name, away_pitcher_hand = parse_pitcher(data.get("awayPitchers", []))
    home_pitcher_name, home_pitcher_hand = parse_pitcher(data.get("homePitchers", []))

    return {
        "away":            parse_batters(away_batters),
        "home":            parse_batters(home_batters),
        "awayReady":       len(away_batters) > 0,
        "homeReady":       len(home_batters) > 0,
        "awayPitcherName": away_pitcher_name,
        "awayPitcherHand": away_pitcher_hand,
        "homePitcherName": home_pitcher_name,
        "homePitcherHand": home_pitcher_hand,
    }


def get_already_confirmed(today):
    try:
        res = (
            supabase.table("projected_lineups")
            .select("team")
            .eq("game_date", today)
            .eq("lineup_status", "Confirmed")
            .execute()
        )
        return {row["team"] for row in (res.data or [])}
    except Exception as e:
        logging.warning(f"Could not fetch confirmed teams: {e}")
        return set()


def upsert_lineup(team_abbr, side, game, lineup_data, status, today, scrape_ts):
    """Write one team's confirmed lineup to Supabase, overwriting any existing row."""
    batting_side = "away" if side == "Away" else "home"

    record = {
        "team":          team_abbr,
        "side":          side,
        "game_date":     today,
        "game_time":     game["gameTime"],
        "lineup_status": status,
        "pitcher_name":  lineup_data[f"{batting_side}PitcherName"],   # own pitcher
        "pitcher_hand":  lineup_data[f"{batting_side}PitcherHand"],
        "batting_order": json.dumps(lineup_data[batting_side]),
        "scrape_date":   scrape_ts,
    }

    supabase.table("projected_lineups").upsert(
        record,
        on_conflict="team,game_date"
    ).execute()
    logging.info(f"  Upserted {team_abbr} ({side}) → {status}")


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    # Compute today inside main() so it's always the correct date at run time
    now_et     = datetime.now(_tz)
    today      = now_et.strftime('%Y-%m-%d')
    scrape_ts  = now_et.strftime("%Y-%m-%d %H:%M:%S %Z")

    logging.info(
        f"[confirmed-lineups] Hourly check for {today} "
        f"(ET: {now_et.strftime('%I:%M %p %Z')})"
    )

    # 1. Always delete rows older than today — keep Supabase clean
    supabase.table("projected_lineups").delete().lt("game_date", today).execute()
    logging.info("Cleared rows older than today")

    # 2. Only check lineups between 11 AM and 9 PM ET
    window_start = now_et.replace(hour=11, minute=0, second=0, microsecond=0)
    window_end   = now_et.replace(hour=21, minute=0, second=0, microsecond=0)
    if not (window_start <= now_et <= window_end):
        logging.info(
            f"Outside confirmation window (11 AM–9 PM ET). "
            f"Now: {now_et.strftime('%I:%M %p %Z')} — skipping lineup checks."
        )
        return

    # 3. Get today's schedule
    games = get_todays_games(today)
    if not games:
        logging.info("No games today — exiting")
        return

    # 4. Track already-confirmed teams so we don't re-send Telegram
    already_confirmed = get_already_confirmed(today)
    logging.info(f"Already confirmed: {already_confirmed or 'none'}")

    newly_confirmed = []

    # 5. Check each game for confirmed lineups
    for game in games:
        pk        = game["gamePk"]
        away_abbr = game["awayAbbr"]
        home_abbr = game["homeAbbr"]

        logging.info(f"Checking {away_abbr} @ {home_abbr} (gamePk={pk})")

        lineup = get_lineup(pk)

        if not lineup:
            logging.info(f"  {away_abbr} / {home_abbr} — not yet posted")
            continue

        # Confirm whichever side(s) are ready — handles partial/doubleheader lineups
        for abbr, side, full_name, ready_key in [
            (away_abbr, "Away", game["awayName"], "awayReady"),
            (home_abbr, "Home", game["homeName"], "homeReady"),
        ]:
            if not lineup[ready_key]:
                logging.info(f"  {abbr} ({side}) — not yet posted, skipping")
                continue
            if abbr in already_confirmed:
                logging.info(f"  {abbr} ({side}) — already confirmed, skipping")
                continue

            upsert_lineup(abbr, side, game, lineup, "Confirmed", today, scrape_ts)
            newly_confirmed.append((abbr, full_name, side, game["gameTime"]))

    # 6. Send Telegram for each newly confirmed team
    for abbr, full_name, side, game_time in newly_confirmed:
        msg = (
            f"✅ Lineup CONFIRMED\n"
            f"{full_name} ({abbr}) — {side}\n"
            f"🕐 {game_time}"
        )
        send_telegram(msg)
        logging.info(f"Telegram sent for {abbr}")

    if not newly_confirmed:
        logging.info("No new confirmations this run — no Telegram sent")

    logging.info("Done")


if __name__ == "__main__":
    main()
