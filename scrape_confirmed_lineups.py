import os
import json
import logging
import pytz
import unicodedata
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

_tz    = pytz.timezone('America/New_York')
_today = datetime.now(_tz)
TODAY  = _today.strftime('%Y-%m-%d')

MLB_API = "https://statsapi.mlb.com/api/v1"

def normalize_name(name):
    """
    Strip accents and convert to plain ASCII.
    e.g. Ronald Acuna Jr. (not Acuna with tilde)
    """
    if not name:
        return name
    nfkd = unicodedata.normalize('NFKD', name)
    return ''.join(c for c in nfkd if not unicodedata.combining(c))

# TEAM NORMALIZATION
# Maps MLB Stats API team names to your standard abbreviations
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

POSITION_MAP = {
    1: "SP", 2: "C",  3: "1B", 4: "2B", 5: "3B",
    6: "SS", 7: "LF", 8: "CF", 9: "RF", 10: "DH",
}

def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=10
        )
    except Exception as e:
        logging.warning(f"Telegram send failed: {e}")

def get_todays_games():
    """Return list of dicts with gamePk, awayAbbr, homeAbbr, gameTime for today."""
    url = f"{MLB_API}/schedule?sportId=1&date={TODAY}&hydrate=team"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    data = r.json()
    games = []
    for date_block in data.get("dates", []):
        for g in date_block.get("games", []):
            away_name = g["teams"]["away"]["team"]["name"]
            home_name = g["teams"]["home"]["team"]["name"]
            away_abbr = MLB_NAME_TO_ABBR.get(away_name)
            home_abbr = MLB_NAME_TO_ABBR.get(home_name)
            if not away_abbr or not home_abbr:
                logging.warning(f"Unknown team name: {away_name} or {home_name} - skipping")
                continue
            game_time_raw = g.get("gameDate", "")
            try:
                gt_utc = datetime.strptime(game_time_raw, "%Y-%m-%dT%H:%M:%SZ")
                gt_utc = pytz.utc.localize(gt_utc)
                gt_et  = gt_utc.astimezone(_tz)
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
    logging.info(f"Found {len(games)} games today ({TODAY})")
    return games

def get_lineup(game_pk):
    """
    Fetch confirmed lineup from MLB Stats API.
    Returns dict with away/home batters and pitchers, or None if not yet posted.
    """
    url = f"{MLB_API}/game/{game_pk}/lineups"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logging.warning(f"gamePk {game_pk} lineups fetch error: {e}")
        return None

    home_batters = data.get("homeBatters", [])
    away_batters = data.get("awayBatters", [])

    if not home_batters or not away_batters:
        return None

    def parse_batters(batters):
        order = []
        for i, p in enumerate(batters):
            pos_code = p.get("primaryPosition", {}).get("code", "")
            pos_name = p.get("primaryPosition", {}).get("abbreviation", pos_code)
            bat_side = p.get("batSide", {}).get("code", "")
            order.append({
                "order":    i + 1,
                "name":     normalize_name(p.get("fullName", "")),
                "position": pos_name,
                "bat_side": bat_side,
            })
        return order

    def parse_pitcher(pitcher_list):
        if not pitcher_list:
            return None, None
        p = pitcher_list[0]
        name       = normalize_name(p.get("fullName", ""))
        pitch_hand = p.get("pitchHand", {}).get("code", "")
        return name, pitch_hand

    away_pitchers = data.get("awayPitchers", [])
    home_pitchers = data.get("homePitchers", [])

    away_pitcher_name, away_pitcher_hand = parse_pitcher(away_pitchers)
    home_pitcher_name, home_pitcher_hand = parse_pitcher(home_pitchers)

    return {
        "away":            parse_batters(away_batters),
        "home":            parse_batters(home_batters),
        "awayPitcherName": away_pitcher_name,
        "awayPitcherHand": away_pitcher_hand,
        "homePitcherName": home_pitcher_name,
        "homePitcherHand": home_pitcher_hand,
    }

def get_already_confirmed():
    """
    Return set of team abbrs already marked Confirmed in Supabase for today.
    Used to avoid re-sending Telegram notifications on subsequent runs.
    """
    try:
        res = supabase.table("projected_lineups") \
            .select("team") \
            .eq("game_date", TODAY) \
            .eq("lineup_status", "Confirmed") \
            .execute()
        return {row["team"] for row in (res.data or [])}
    except Exception as e:
        logging.warning(f"Could not fetch already-confirmed teams: {e}")
        return set()

def upsert_lineup(team_abbr, side, game, lineup_data, status):
    batting_side  = "away" if side == "Away" else "home"
    batting_order = lineup_data[batting_side]
    pitcher_name  = lineup_data[f"{batting_side}PitcherName"]
    pitcher_hand  = lineup_data[f"{batting_side}PitcherHand"]

    record = {
        "team":          team_abbr,
        "side":          side,
        "game_date":     TODAY,
        "game_time":     game["gameTime"],
        "lineup_status": status,
        "pitcher_name":  pitcher_name,
        "pitcher_hand":  pitcher_hand,
        "batting_order": json.dumps(batting_order),
        "scrape_date":   _today.strftime("%Y-%m-%d %H:%M:%S %Z"),
    }

    supabase.table("projected_lineups").upsert(
        record,
        on_conflict="team,game_date"
    ).execute()

# MAIN
def main():
    logging.info(f"[confirmed-lineups] Hourly check for {TODAY} "
                 f"(ET: {datetime.now(_tz).strftime('%I:%M %p %Z')})")

    # ── 1. Clean up old dates (keep only today) ──────────────────────────────
    supabase.table("projected_lineups").delete().lt("game_date", TODAY).execute()
    logging.info("Cleared rows older than today")

    # ── 2. Get today's schedule from MLB API ─────────────────────────────────
    games = get_todays_games()
    if not games:
        logging.info("No games today — exiting")
        return

    # ── 3. Find which teams are already Confirmed in Supabase ────────────────
    #    Used to suppress duplicate Telegram notifications across hourly runs.
    already_confirmed = get_already_confirmed()
    logging.info(f"Already confirmed in Supabase: {already_confirmed or 'none'}")

    newly_confirmed = []   # teams flipping from Projected → Confirmed this run

    # ── 4. Per-game: fetch lineup from MLB API ───────────────────────────────
    for game in games:
        pk        = game["gamePk"]
        away_abbr = game["awayAbbr"]
        home_abbr = game["homeAbbr"]
        away_name = game["awayName"]
        home_name = game["homeName"]

        logging.info(f"Checking {away_abbr} @ {home_abbr} (gamePk={pk})")

        lineup = get_lineup(pk)

        if lineup:
            # Overwrite whatever exists in Supabase (projected or confirmed)
            # with the official confirmed lineup.
            for abbr, side, name in [
                (away_abbr, "Away", away_name),
                (home_abbr, "Home", home_name),
            ]:
                upsert_lineup(abbr, side, game, lineup, "Confirmed")
                logging.info(f"  {abbr} ({side}) → Confirmed — upserted")

                if abbr not in already_confirmed:
                    newly_confirmed.append((abbr, name, side, game["gameTime"]))
        else:
            # Not confirmed yet — do NOT touch the existing projected row.
            for abbr, side in [(away_abbr, "Away"), (home_abbr, "Home")]:
                logging.info(f"  {abbr} ({side}) not yet confirmed — leaving projected row intact")

    # ── 5. Send Telegram for every team newly confirmed this run ─────────────
    for abbr, name, side, game_time in newly_confirmed:
        msg = (
            f"✅ Lineup CONFIRMED\n"
            f"{name} ({abbr}) — {side}\n"
            f"🕐 {game_time}"
        )
        send_telegram(msg)
        logging.info(f"Telegram sent for {abbr}")

    if not newly_confirmed:
        logging.info("No new confirmations this run — no Telegram sent")

    logging.info("Done")

if __name__ == "__main__":
    main()
