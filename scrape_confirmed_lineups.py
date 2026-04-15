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

_tz    = pytz.timezone('America/New_York')
_today = datetime.now(_tz)
TODAY  = _today.strftime('%Y-%m-%d')

MLB_API = "https://statsapi.mlb.com/api/v1"

# ── NAME DISPLAY MAP ──────────────────────────────────────────────────────────
# MLB Stats API returns names without accents and sometimes without Jr./Sr.
# This map patches them to match fangraphs_player_splits exactly.
# Add entries here whenever a "Not Found" shows up in Lineup Stats.

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
    """
    Return canonical display name matching fangraphs_player_splits.
    Looks up DISPLAY_NAME_MAP first, falls back to whitespace-collapsed raw name.
    Never strips Jr./Sr. — the stats table needs them for matching.
    """
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


# ── MLB API HELPERS ───────────────────────────────────────────────────────────
def get_todays_games():
    """Return list of game dicts for today from the MLB Stats API."""
    url = f"{MLB_API}/schedule?sportId=1&date={TODAY}&hydrate=team"
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

            # Parse game time UTC → ET
            game_time_raw = g.get("gameDate", "")
            try:
                gt_utc    = datetime.strptime(game_time_raw, "%Y-%m-%dT%H:%M:%SZ")
                gt_utc    = pytz.utc.localize(gt_utc)
                gt_et     = gt_utc.astimezone(_tz)
                # %-I is Linux-only but GitHub Actions runs ubuntu-latest so this is fine
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
    Fetch confirmed lineup from MLB Stats API /game/{pk}/lineups.
    Returns structured dict or None if lineup not yet posted.
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

    if not home_batters or not away_batters:
        return None  # lineup not posted yet

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
        """Returns (name, hand) tuple. Call once and unpack."""
        if not pitcher_list:
            return None, None
        p = pitcher_list[0]
        return (
            normalize_name(p.get("fullName", "")),
            p.get("pitchHand", {}).get("code", ""),
        )

    # FIX: call parse_pitcher once per side and unpack — not twice
    away_pitcher_name, away_pitcher_hand = parse_pitcher(data.get("awayPitchers", []))
    home_pitcher_name, home_pitcher_hand = parse_pitcher(data.get("homePitchers", []))

    return {
        "away":            parse_batters(away_batters),
        "home":            parse_batters(home_batters),
        "awayPitcherName": away_pitcher_name,
        "awayPitcherHand": away_pitcher_hand,
        "homePitcherName": home_pitcher_name,
        "homePitcherHand": home_pitcher_hand,
    }


def get_already_confirmed():
    """Return set of team abbrs already Confirmed in Supabase today."""
    try:
        res = (
            supabase.table("projected_lineups")
            .select("team")
            .eq("game_date", TODAY)
            .eq("lineup_status", "Confirmed")
            .execute()
        )
        return {row["team"] for row in (res.data or [])}
    except Exception as e:
        logging.warning(f"Could not fetch confirmed teams: {e}")
        return set()


def upsert_lineup(team_abbr, side, game, lineup_data, status):
    """Write one team's confirmed lineup to Supabase, overwriting any existing row."""
    batting_side = "away" if side == "Away" else "home"

    record = {
        "team":          team_abbr,
        "side":          side,
        "game_date":     TODAY,
        "game_time":     game["gameTime"],
        "lineup_status": status,
        "pitcher_name":  lineup_data[f"{batting_side}PitcherName"],
        "pitcher_hand":  lineup_data[f"{batting_side}PitcherHand"],
        "batting_order": json.dumps(lineup_data[batting_side]),
        "scrape_date":   _today.strftime("%Y-%m-%d %H:%M:%S %Z"),
    }

    supabase.table("projected_lineups").upsert(
        record,
        on_conflict="team,game_date"
    ).execute()
    logging.info(f"  Upserted {team_abbr} ({side}) → {status}")


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    logging.info(
        f"[confirmed-lineups] Hourly check for {TODAY} "
        f"(ET: {datetime.now(_tz).strftime('%I:%M %p %Z')})"
    )

    # 1. Delete rows older than today (keep Supabase clean)
    supabase.table("projected_lineups").delete().lt("game_date", TODAY).execute()
    logging.info("Cleared rows older than today")

    # 2. Get today's schedule
    games = get_todays_games()
    if not games:
        logging.info("No games today — exiting")
        return

    # 3. Track which teams are already confirmed so we don't re-send Telegram
    already_confirmed = get_already_confirmed()
    logging.info(f"Already confirmed: {already_confirmed or 'none'}")

    newly_confirmed = []

    # 4. Check each game for a confirmed lineup
    for game in games:
        pk        = game["gamePk"]
        away_abbr = game["awayAbbr"]
        home_abbr = game["homeAbbr"]

        logging.info(f"Checking {away_abbr} @ {home_abbr} (gamePk={pk})")

        lineup = get_lineup(pk)

        if lineup:
            for abbr, side, full_name in [
                (away_abbr, "Away", game["awayName"]),
                (home_abbr, "Home", game["homeName"]),
            ]:
                upsert_lineup(abbr, side, game, lineup, "Confirmed")
                if abbr not in already_confirmed:
                    newly_confirmed.append((abbr, full_name, side, game["gameTime"]))
        else:
            logging.info(f"  {away_abbr} / {home_abbr} — not yet confirmed, projected rows untouched")

    # 5. Send Telegram for each newly confirmed team
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
