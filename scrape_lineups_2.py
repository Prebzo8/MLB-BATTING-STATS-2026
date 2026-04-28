"""
scrape_lineups.py  —  Single scraper for all MLB lineups (projected + confirmed)
Source: https://www.rotowire.com/baseball/daily-lineups.php
"""

import os
import re
import sys
import time
import json
import logging
import pytz
import requests
from datetime import datetime
from supabase import create_client, Client
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

supabase: Client = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"]
)

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

_tz = pytz.timezone('America/New_York')

ROTOWIRE_URL = "https://www.rotowire.com/baseball/daily-lineups.php"

# ── ABBREVIATION MAP ──────────────────────────────────────────────────────
# Keys  = what RotoWire emits in .lineup__abbr
# Values = canonical Baseball-Reference / MLB Stats API codes
RW_ABBR_MAP = {
    "AZ":  "ARI",   # Arizona Diamondbacks (RotoWire alternate)
    "KC":  "KCR",   # Kansas City Royals
    "SD":  "SDP",   # San Diego Padres
    "SF":  "SFG",   # San Francisco Giants
    "TB":  "TBR",   # Tampa Bay Rays
    "CWS": "CHW",   # Chicago White Sox
    "WAS": "WSN",   # Washington Nationals
    "WSH": "WSN",   # Washington Nationals (alternate)
    "OAK": "ATH",   # legacy Oakland code
    "SAC": "ATH",   # Sacramento placeholder (2025–2027)
    "LV":  "ATH",   # Las Vegas (future-proofing)
}

# Teams that pass through unchanged (for reference / documentation):
# ARI, ATL, BAL, BOS, CHC, CIN, CLE, COL, DET, HOU,
# LAA, LAD, MIA, MIL, MIN, NYM, NYY, PHI, PIT, SEA,
# STL, TEX, TOR, WSN — all emitted correctly by RotoWire already.

def normalize_abbr(abbr):
    if not abbr:
        return abbr
    abbr = abbr.strip().upper()
    normalized = RW_ABBR_MAP.get(abbr, abbr)
    if len(normalized) != 3:
        logging.warning(f"Unexpected team abbr after normalization: '{normalized}' (from '{abbr}')")
    return normalized


# ── NAME HELPERS ──────────────────────────────────────────────────────────

def slug_to_name(href):
    """
    Convert a RotoWire player href slug to a full display name.
    e.g. "/baseball/player/nick-martinez-22541" → "Nick Martinez"
    The slug is always the full name — never abbreviated like "C. Simpson".
    """
    if not href:
        return None
    m = re.search(r'/player/([a-z0-9-]+?)(?:-\d+)?/?$', href)
    if not m:
        return None
    slug = m.group(1)                              # e.g. "nick-martinez"
    name = ' '.join(w.capitalize() for w in slug.split('-'))
    return name  # e.g. "Nick Martinez"


# ── NAME DISPLAY MAP ──────────────────────────────────────────────────────
# Corrects accents / suffixes that are lost when reconstructing from URL slugs.
# Keys use the slug-reconstructed form (no accents, no trailing period on Jr).
DISPLAY_NAME_MAP = {
    "Ronald Acuna Jr":       "Ronald Acuña Jr.",
    "Ronald Acuna":          "Ronald Acuña Jr.",
    "Fernando Tatis Jr":     "Fernando Tatis Jr.",
    "Fernando Tatis":        "Fernando Tatis Jr.",
    "Bobby Witt Jr":         "Bobby Witt Jr.",
    "Bobby Witt":            "Bobby Witt Jr.",
    "Vladimir Guerrero Jr":  "Vladimir Guerrero Jr.",
    "Vladimir Guerrero":     "Vladimir Guerrero Jr.",
    "Jazz Chisholm Jr":      "Jazz Chisholm Jr.",
    "Jazz Chisholm":         "Jazz Chisholm Jr.",
    "Luis Robert Jr":        "Luis Robert Jr.",
    "Luis Robert":           "Luis Robert Jr.",
    "Luis Garcia Jr":        "Luis García Jr.",
    "Luis Garcia":           "Luis García Jr.",
    "Jose Abreu":            "José Abreu",
    "Jose Ramirez":          "José Ramírez",
    "Yordan Alvarez":        "Yordan Álvarez",
    "Julio Rodriguez":       "Julio Rodríguez",
    "Eloy Jimenez":          "Eloy Jiménez",
    "Jose Trevino":          "José Treviño",
    "Lourdes Gurriel Jr":    "Lourdes Gurriel Jr.",
    "Lourdes Gurriel":       "Lourdes Gurriel Jr.",
    "Adolis Garcia":         "Adolis García",
    "Aledmys Diaz":          "Aledmys Díaz",
    "Victor Robles":         "Víctor Robles",
    "Victor Caratini":       "Víctor Caratini",
    "Jose Miranda":          "José Miranda",
    "Jose Iglesias":         "José Iglesias",
    "Jose Siri":             "José Siri",
    "Enrique Hernandez":     "Enrique Hernández",
    "Yandy Diaz":            "Yandy Díaz",
    "Yainer Diaz":           "Yainer Díaz",
    "Cristian Pache":        "Cristian Paché",
}

def normalize_name(name):
    """Apply DISPLAY_NAME_MAP corrections; return name unchanged if no match."""
    if not name:
        return name
    cleaned = ' '.join(name.strip().split())
    return DISPLAY_NAME_MAP.get(cleaned, cleaned)


def best_name(anchor_el):
    """
    Return the best full player name from a <a> element:
      1. title attribute  — full name if RotoWire sets it
      2. href slug        — always full first+last, never abbreviated
      3. text content     — last resort; may be abbreviated like "C. Simpson"
    Then run through normalize_name for accent corrections.
    """
    if anchor_el is None:
        return ""

    # 1. title attribute
    title = (anchor_el.get("title") or "").strip()
    # Accept title only if it looks like a real full name (not "C. Simpson")
    if title and len(title) > 4 and not re.match(r'^[A-Z]\.\s', title):
        return normalize_name(title)

    # 2. href slug → reconstructed full name (most reliable)
    slug_name = slug_to_name(anchor_el.get("href", ""))
    if slug_name:
        return normalize_name(slug_name)

    # 3. Text fallback
    return normalize_name(anchor_el.get_text(strip=True))

# ── TELEGRAM ──────────────────────────────────────────────────────────────
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

# ── SCRAPER ───────────────────────────────────────────────────────────────
def scrape_rotowire():
    """Scrape RotoWire lineups page. Returns list of team records."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ))

        logging.info(f"Loading {ROTOWIRE_URL}")
        page.goto(ROTOWIRE_URL, wait_until="domcontentloaded", timeout=90000)

        # Give dynamic content (lineups) a moment to render
        page.wait_for_timeout(3000)

        try:
            page.wait_for_selector(".lineup", timeout=30000)
            logging.info("Page ready — .lineup found")
        except Exception:
            logging.warning("Timed out waiting for .lineup — proceeding anyway")

        html = page.content()
        browser.close()

    # Save debug artifact
    try:
        with open("rotowire_debug.html", "w", encoding="utf-8") as f:
            f.write(html)
        logging.info(f"Debug HTML saved ({len(html):,} chars)")
    except Exception as e:
        logging.warning(f"Could not save debug HTML: {e}")

    soup  = BeautifulSoup(html, "lxml")
    cards = soup.select(".lineup")
    logging.info(f"Found {len(cards)} lineup cards")

    results = []
    for card in cards:
        try:
            records = parse_lineup_card(card)
            results.extend(records)
        except Exception as e:
            logging.warning(f"Card parse error: {e}", exc_info=True)

    return results


def scrape_with_retry(retries=3, delay=15):
    """Wrap scrape_rotowire with retry logic. Returns records or empty list."""
    for attempt in range(1, retries + 1):
        try:
            records = scrape_rotowire()
            if records:
                return records
            logging.warning(f"Attempt {attempt}/{retries}: scrape returned 0 records")
        except Exception as e:
            logging.warning(f"Attempt {attempt}/{retries} failed: {e}")
        if attempt < retries:
            logging.info(f"Retrying in {delay}s…")
            time.sleep(delay)
    logging.error("All scrape attempts failed — returning empty list")
    return []


# ── PARSER ────────────────────────────────────────────────────────────────
def parse_lineup_card(card):
    """Parse one .lineup card into away + home records."""
    now_et = datetime.now(_tz)
    today  = now_et.strftime('%Y-%m-%d')
    ts     = now_et.strftime("%Y-%m-%d %H:%M:%S %Z")

    time_el   = card.select_one(".lineup__time")
    game_time = time_el.get_text(strip=True) if time_el else ""

    team_els = card.select(".lineup__abbr")
    if len(team_els) < 2:
        logging.warning("Could not find 2 team abbrs — skipping")
        return []
    away_abbr = normalize_abbr(team_els[0].get_text(strip=True))
    home_abbr = normalize_abbr(team_els[1].get_text(strip=True))

    away_list = card.select_one(".lineup__list.is-visit")
    home_list = card.select_one(".lineup__list.is-home")

    if not away_list or not home_list:
        logging.warning(f"{away_abbr} @ {home_abbr} — missing lineup lists, skipping")
        return []

    def parse_side(ul, side_label):
        ph           = ul.select_one(".lineup__player-highlight")
        pitcher_name = None
        pitcher_hand = None

        if ph:
            # ── Pitcher name ──────────────────────────────────────────────
            name_el  = ph.select_one(".lineup__player-highlight-name")
            raw_name = name_el.get_text(strip=True) if name_el else ""

            if not raw_name:
                logging.warning(f"  {side_label} — pitcher name element found but empty")
            else:
                # Try href slug first — gives clean unabbreviated name
                anchor    = (name_el.select_one("a") if name_el else None) or ph.select_one("a")
                slug_name = slug_to_name(anchor.get("href", "") if anchor else "")
                if slug_name:
                    pitcher_name = normalize_name(slug_name)
                else:
                    # FIX: \s* (zero-or-more spaces) so "MartinezR" (no space
                    # between name and hand letter) is correctly stripped.
                    cleaned = re.sub(r'\s*[LRS]$', '', raw_name).strip()
                    pitcher_name = normalize_name(cleaned)

            # ── Pitcher hand ──────────────────────────────────────────────
            hand_el = ph.select_one(".lineup__throws")
            if hand_el:
                h = hand_el.get_text(strip=True).upper()
                if h in ("L", "R"):
                    pitcher_hand = h
                else:
                    logging.warning(f"  {side_label} — unexpected pitcher hand value: '{h}'")
            else:
                # Fallback: pull hand letter from end of raw_name.
                # Works whether there's a space or not ("Martinez R" / "MartinezR").
                m = re.search(r'\s*([LR])$', raw_name)
                if m:
                    pitcher_hand = m.group(1)

            if pitcher_hand is None:
                logging.warning(
                    f"  {side_label} — could not determine pitcher hand "
                    f"(raw='{raw_name}')"
                )

        status_el = ul.select_one(".lineup__status")
        status    = "Confirmed" if status_el and "is-confirmed" in status_el.get("class", []) else "Projected"

        # ── Batting order ─────────────────────────────────────────────────
        order = []
        for i, player_el in enumerate(ul.select(".lineup__player")):
            pos_el  = player_el.select_one(".lineup__pos")
            name_el = player_el.select_one("a")
            bats_el = player_el.select_one(".lineup__bats")

            # FIX: best_name() prefers href slug over display text so
            # abbreviated names like "C. Simpson" become "Christian Simpson".
            name = best_name(name_el)
            if not name:
                continue
            order.append({
                "order":    i + 1,
                "name":     name,
                "position": pos_el.get_text(strip=True) if pos_el else "",
                "bat_side": bats_el.get_text(strip=True) if bats_el else "",
            })

        # Format pitcher_name as "Nick Martinez (R)"
        if pitcher_name and pitcher_hand:
            pitcher_display = f"{pitcher_name} ({pitcher_hand})"
        elif pitcher_name:
            pitcher_display = pitcher_name
        else:
            pitcher_display = None

        logging.info(
            f"  {side_label} | {status} | "
            f"pitcher={pitcher_display} | batters={len(order)}"
        )
        return {
            "lineup_status": status,
            "pitcher_name":  pitcher_display,
            "pitcher_hand":  pitcher_hand,
            "batting_order": order,
        }

    logging.info(f"{away_abbr} @ {home_abbr} | {game_time}")
    away_data = parse_side(away_list, away_abbr)
    home_data = parse_side(home_list, home_abbr)

    return [
        {
            "team":          away_abbr,
            "side":          "Away",
            "game_date":     today,
            "game_time":     game_time,
            "lineup_status": away_data["lineup_status"],
            "pitcher_name":  away_data["pitcher_name"],
            "pitcher_hand":  away_data["pitcher_hand"],
            "batting_order": away_data["batting_order"],
            "scrape_date":   ts,
        },
        {
            "team":          home_abbr,
            "side":          "Home",
            "game_date":     today,
            "game_time":     game_time,
            "lineup_status": home_data["lineup_status"],
            "pitcher_name":  home_data["pitcher_name"],
            "pitcher_hand":  home_data["pitcher_hand"],
            "batting_order": home_data["batting_order"],
            "scrape_date":   ts,
        },
    ]


# ── SUPABASE ──────────────────────────────────────────────────────────────
def get_already_confirmed(today):
    try:
        res = supabase.table("projected_lineups") \
            .select("team") \
            .eq("game_date", today) \
            .eq("lineup_status", "Confirmed") \
            .execute()
        return {row["team"] for row in (res.data or [])}
    except Exception as e:
        logging.warning(f"Could not fetch confirmed teams: {e}")
        return set()


def write_to_supabase(records, today):
    if not records:
        logging.info("No records to write")
        return

    supabase.table("projected_lineups").delete().lt("game_date", today).execute()
    logging.info("Cleared rows older than today")

    already_confirmed = get_already_confirmed(today)

    confirmed_records = [r for r in records if r["lineup_status"] == "Confirmed"]
    projected_records = [r for r in records if r["lineup_status"] == "Projected"]

    # ── Write confirmed records ────────────────────────────────────────────
    newly_confirmed = []
    if confirmed_records:
        seen        = {(r["team"], r["game_date"]): r for r in confirmed_records}
        conf_deduped = list(seen.values())

        supabase.table("projected_lineups") \
            .upsert(conf_deduped, on_conflict="team,game_date") \
            .execute()
        logging.info(f"Upserted {len(conf_deduped)} confirmed records")

        for r in conf_deduped:
            if r["team"] not in already_confirmed:
                newly_confirmed.append(r)

    # ── Write projected records (only for teams not already confirmed) ─────
    confirmed_teams = {r["team"] for r in confirmed_records} | already_confirmed
    proj_to_insert  = [r for r in projected_records if r["team"] not in confirmed_teams]

    if proj_to_insert:
        # Scope the delete to only the teams we're about to replace — safe if
        # the scrape returns a partial set due to a transient parse failure.
        teams_being_replaced = [r["team"] for r in proj_to_insert]
        supabase.table("projected_lineups") \
            .delete() \
            .eq("game_date", today) \
            .eq("lineup_status", "Projected") \
            .in_("team", teams_being_replaced) \
            .execute()
        logging.info(f"Cleared projected rows for {len(teams_being_replaced)} teams")

        seen         = {(r["team"], r["game_date"]): r for r in proj_to_insert}
        proj_deduped = list(seen.values())

        supabase.table("projected_lineups") \
            .upsert(proj_deduped, on_conflict="team,game_date") \
            .execute()
        logging.info(f"Upserted {len(proj_deduped)} projected records")

    # ── Telegram notifications for newly confirmed lineups ─────────────────
    for r in newly_confirmed:
        msg = f"✅ Lineup CONFIRMED\n{r['team']} — {r['side']}\n🕐 {r['game_time']}"
        send_telegram(msg)
        logging.info(f"Telegram sent for {r['team']}")

    if not newly_confirmed:
        logging.info("No new confirmations this run")


# ── MAIN ──────────────────────────────────────────────────────────────────
def main():
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
        f"Scraping RotoWire lineups for {today} "
        f"(ET: {now_et.strftime('%I:%M %p %Z')})"
        + (" [FORCED]" if force else "")
    )

    records = scrape_with_retry(retries=3, delay=15)
    logging.info(f"Parsed {len(records)} team records")

    confirmed = sum(1 for r in records if r["lineup_status"] == "Confirmed")
    projected = sum(1 for r in records if r["lineup_status"] == "Projected")
    logging.info(f"  Confirmed: {confirmed} | Projected: {projected}")

    write_to_supabase(records, today)
    logging.info("Done")


if __name__ == "__main__":
    main()
