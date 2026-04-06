import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
from supabase import create_client, Client
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

supabase: Client = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"]
)

# ── CONFIG ───────────────────────────────────────────────────
_tz    = pytz.timezone('America/New_York')
_today = datetime.now(_tz)
TODAY  = _today.strftime('%Y-%m-%d')

MLB_IDS = {
    'ARI':109,'ATH':133,'ATL':144,'BAL':110,'BOS':111,
    'CHC':112,'CHW':145,'CIN':113,'CLE':114,'COL':115,
    'DET':116,'HOU':117,'KCR':118,'LAA':108,'LAD':119,
    'MIA':146,'MIL':158,'MIN':142,'NYM':121,'NYY':147,
    'PHI':143,'PIT':134,'SDP':135,'SEA':136,'SFG':137,
    'STL':138,'TBR':139,'TEX':140,'TOR':141,'WSN':120,
}
ID_TO_ABBR = {v: k for k, v in MLB_IDS.items()}

DATE_RANGES = {
    'Full Year':    {'type': 'season',      'start': '2026-03-27', 'end': TODAY},
    'Last 30 Days': {'type': 'byDateRange', 'start': (_today - timedelta(days=30)).strftime('%Y-%m-%d'), 'end': TODAY},
    'Last 14 Days': {'type': 'byDateRange', 'start': (_today - timedelta(days=14)).strftime('%Y-%m-%d'), 'end': TODAY},
    'Last 7 Days':  {'type': 'byDateRange', 'start': (_today - timedelta(days=7)).strftime('%Y-%m-%d'),  'end': TODAY},
}

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

# ── FETCH ────────────────────────────────────────────────────
def fetch_rpg(date_range_label, cfg):
    logging.info(f"📥 Fetching R/G — {date_range_label} ({cfg['start']} → {cfg['end']})")

    if cfg['type'] == 'season':
        url = (
            "https://statsapi.mlb.com/api/v1/teams/stats"
            "?season=2026&stats=season&group=hitting&gameType=R&sportId=1"
        )
    else:
        url = (
            "https://statsapi.mlb.com/api/v1/teams/stats"
            f"?season=2026&stats=byDateRange&group=hitting&gameType=R&sportId=1"
            f"&startDate={cfg['start']}&endDate={cfg['end']}"
        )

    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    splits = data.get('stats', [{}])[0].get('splits', [])
    if not splits:
        logging.warning(f"  No splits returned for {date_range_label}")
        return []

    records = []
    for split in splits:
        team_id = split.get('team', {}).get('id')
        abbr    = ID_TO_ABBR.get(team_id)

        # Skip non-MLB teams
        if not abbr:
            continue

        stat      = split.get('stat', {})
        runs      = float(stat.get('runs', 0) or 0)
        games     = float(stat.get('gamesPlayed', 0) or 0)
        rpg       = round(runs / games, 3) if games > 0 else None

        records.append({
            'team':          abbr,
            'split_type':    'No Splits',
            'date_range':    date_range_label,
            'runs':          int(runs),
            'games_played':  int(games),
            'runs_per_game': rpg,
            'scrape_date':   _today.strftime('%Y-%m-%d %H:%M:%S %Z'),
        })
        logging.info(f"  {abbr}: {int(runs)} R / {int(games)} G = {rpg} R/G")

    # Warn about any missing teams
    found   = {r['team'] for r in records}
    missing = set(MLB_IDS.keys()) - found
    if missing:
        logging.warning(f"  Missing teams for {date_range_label}: {sorted(missing)}")

    logging.info(f"  ✅ {len(records)} teams fetched for {date_range_label}")
    return records

# ── MAIN ─────────────────────────────────────────────────────
all_records = []

for label, cfg in DATE_RANGES.items():
    records = fetch_rpg(label, cfg)
    all_records.extend(records)
    time.sleep(2)

if not all_records:
    logging.error("No records fetched — aborting.")
    raise SystemExit(1)

logging.info(f"\n📊 Total records: {len(all_records)}")

# ── UPSERT TO SUPABASE ────────────────────────────────────────
for label in DATE_RANGES.keys():
    supabase.table("team_runs_per_game").delete().eq(
        "split_type", "No Splits"
    ).eq(
        "date_range", label
    ).execute()
    logging.info(f"🗑️  Cleared old rows for No Splits | {label}")

supabase.table("team_runs_per_game").upsert(
    all_records, on_conflict="team,split_type,date_range"
).execute()

logging.info(f"✅ {len(all_records)} rows upserted to team_runs_per_game")
