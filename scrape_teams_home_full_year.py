import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import pytz
from supabase import create_client, Client
import logging
import time
from bs4 import BeautifulSoup
from io import StringIO

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

supabase: Client = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"]
)

SPLIT_NAME  = "Home"
DATE_RANGE  = "Full Year"

# --- Dynamic date window ---
_tz    = pytz.timezone('America/New_York')
_today = datetime.now(_tz)
end_date  = _today.strftime('%Y-%m-%d')
start_date = '2026-01-01'  # full season — fixed

URL = "https://www.fangraphs.com/leaders-legacy.aspx?pos=all&stats=bat&lg=all&qual=0&type=c,6,34,35,36,23,21,37,38,39,40,53,41,52,51,50,54&season=2026&month=15&season1=2026&ind=0&team=0,ts&rost=0&age=0&filter=&players=0&startdate=2026-01-01&enddate=2026-12-31&v_cr=legacy"

logging.info(f"📥 Scraping Teams — {SPLIT_NAME} | {DATE_RANGE} ({start_date if 'None' != 'None' else '2026-01-01'}) → {end_date}")

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.fangraphs.com/",
    "DNT": "1",
    "Connection": "keep-alive",
}

resp = requests.get(URL, headers=headers, timeout=20)
resp.raise_for_status()

soup = BeautifulSoup(resp.text, "lxml")
table = soup.find("table", class_="rgMasterTable") or soup.find("table", id=lambda x: x and "dg1" in x.lower())

if not table:
    raise ValueError(f"Could not find leaderboard table for Teams — {SPLIT_NAME} | {DATE_RANGE}")

df = pd.read_html(StringIO(str(table)))[0]

if isinstance(df.columns, pd.MultiIndex):
    df.columns = df.columns.get_level_values(1)

df = df.iloc[1:].reset_index(drop=True)

rank_col = df.columns[0]
df = df[~df[rank_col].astype(str).str.contains('Page size|items in', na=False)].reset_index(drop=True)

logging.info(f"Columns detected: {df.columns.tolist()}")

DESIRED = ['Team', 'PA', 'BB%', 'K%', 'BB/K', 'AVG', 'SB', 'OBP', 'SLG', 'OPS',
           'ISO', 'Spd', 'BABIP', 'wRC', 'wRAA', 'wOBA', 'wRC+']

for col in DESIRED:
    if col not in df.columns:
        logging.warning(f"  Column '{col}' not found — filling with None")
        df[col] = None

df = df[DESIRED].copy()

# ── NORMALIZE: map any FanGraphs full names → standard 3-letter abbreviations ─
TEAM_NAME_MAP = {
    'Arizona Diamondbacks': 'ARI', 'Arizona': 'ARI',
    'Athletics': 'ATH', 'Oakland Athletics': 'ATH', 'Oakland': 'ATH',
    'Atlanta Braves': 'ATL', 'Atlanta': 'ATL',
    'Baltimore Orioles': 'BAL', 'Baltimore': 'BAL',
    'Boston Red Sox': 'BOS', 'Boston': 'BOS',
    'Chicago Cubs': 'CHC',
    'Chicago White Sox': 'CHW',
    'Cincinnati Reds': 'CIN', 'Cincinnati': 'CIN',
    'Cleveland Guardians': 'CLE', 'Cleveland': 'CLE',
    'Colorado Rockies': 'COL', 'Colorado': 'COL',
    'Detroit Tigers': 'DET', 'Detroit': 'DET',
    'Houston Astros': 'HOU', 'Houston': 'HOU',
    'Kansas City Royals': 'KCR', 'Kansas City': 'KCR',
    'Los Angeles Angels': 'LAA',
    'Los Angeles Dodgers': 'LAD',
    'Miami Marlins': 'MIA', 'Miami': 'MIA',
    'Milwaukee Brewers': 'MIL', 'Milwaukee': 'MIL',
    'Minnesota Twins': 'MIN', 'Minnesota': 'MIN',
    'New York Mets': 'NYM',
    'New York Yankees': 'NYY',
    'Philadelphia Phillies': 'PHI', 'Philadelphia': 'PHI',
    'Pittsburgh Pirates': 'PIT', 'Pittsburgh': 'PIT',
    'San Diego Padres': 'SDP', 'San Diego': 'SDP',
    'Seattle Mariners': 'SEA', 'Seattle': 'SEA',
    'San Francisco Giants': 'SFG', 'San Francisco': 'SFG',
    'St. Louis Cardinals': 'STL', 'St. Louis': 'STL',
    'Tampa Bay Rays': 'TBR', 'Tampa Bay': 'TBR',
    'Texas Rangers': 'TEX', 'Texas': 'TEX',
    'Toronto Blue Jays': 'TOR', 'Toronto': 'TOR',
    'Washington Nationals': 'WSN', 'Washington': 'WSN',
}
df['Team'] = df['Team'].replace(TEAM_NAME_MAP)

# ── WHITELIST: keep only the 30 known MLB team abbreviations ──────────────────
VALID_TEAMS = {
    'ARI','ATH','ATL','BAL','BOS','CHC','CHW','CIN','CLE','COL',
    'DET','HOU','KCR','LAA','LAD','MIA','MIL','MIN','NYM','NYY',
    'PHI','PIT','SDP','SEA','SFG','STL','TBR','TEX','TOR','WSN'
}
df = df[df['Team'].isin(VALID_TEAMS)].reset_index(drop=True)
df = df.drop_duplicates(subset=['Team'], keep='first').reset_index(drop=True)
logging.info(f"Teams after normalize+whitelist+dedup ({len(df)}): {sorted(df['Team'].tolist())}")


df.rename(columns={
    'Team': 'team', 'PA': 'pa',
    'BB%': 'bb_pct', 'K%': 'k_pct', 'BB/K': 'bb_k',
    'AVG': 'avg', 'SB': 'sb', 'OBP': 'obp', 'SLG': 'slg', 'OPS': 'ops',
    'ISO': 'iso', 'Spd': 'spd', 'BABIP': 'babip',
    'wRC': 'wrc', 'wRAA': 'wraa', 'wOBA': 'woba', 'wRC+': 'wrc_plus'
}, inplace=True)

for col in ['bb_pct', 'k_pct']:
    df[col] = pd.to_numeric(
        df[col].astype(str).str.replace('%', '', regex=False),
        errors='coerce'
    ) / 100

for col in ['pa', 'sb', 'bb_k', 'avg', 'obp', 'slg', 'ops', 'iso', 'spd', 'babip', 'wrc', 'wraa', 'woba', 'wrc_plus']:
    df[col] = pd.to_numeric(df[col], errors='coerce')

df['pa']   = df['pa'].astype('Int64')
df['sb']   = df['sb'].astype('Int64')
df['wrc']  = df['wrc'].astype('Int64')
df['wraa'] = df['wraa'].round(1)

df['split_type']  = SPLIT_NAME
df['date_range']  = DATE_RANGE
df['scrape_date'] = _today.strftime('%Y-%m-%d %H:%M:%S %Z')

df = df.dropna(subset=['team']).reset_index(drop=True)

records = df.to_dict(orient="records")

for record in records:
    for k, v in record.items():
        if pd.isna(v):
            record[k] = None

# Delete all existing rows for this split + date range before inserting fresh data
supabase.table("fangraphs_team_batting").delete().eq(
    "split_type", SPLIT_NAME
).eq(
    "date_range", DATE_RANGE
).execute()
logging.info(f"🗑️  Cleared old rows for Teams — '{SPLIT_NAME}' | '{DATE_RANGE}'")

supabase.table("fangraphs_team_batting").upsert(
    records, on_conflict="team,split_type,date_range,scrape_date"
).execute()

logging.info(f"✅ {len(df):,} rows upserted for Teams — {SPLIT_NAME} | {DATE_RANGE}")
time.sleep(4)
