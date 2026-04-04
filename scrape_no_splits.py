import os
import pandas as pd
import requests
from datetime import datetime
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

URL = "https://www.fangraphs.com/leaders-legacy.aspx?pos=np&stats=bat&lg=all&qual=5&type=c,6,34,35,36,23,37,38,39,40,53,41,52,51,50,61&season=2026&month=0&season1=2026&ind=0&team=0&rost=0&age=0&filter=&players=0&startdate=2026-01-01&enddate=2026-12-31&v_cr=legacy&page=1_2000"
SPLIT_NAME = "No Splits"

logging.info(f"📥 Scraping {SPLIT_NAME}...")

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
    raise ValueError(f"Could not find leaderboard table for {SPLIT_NAME}")

df = pd.read_html(StringIO(str(table)))[0]

# Flatten MultiIndex and remove the top pager row
if isinstance(df.columns, pd.MultiIndex):
    df.columns = df.columns.get_level_values(1)

df = df.iloc[1:].reset_index(drop=True)

# Remove junk rows — guard against '#' column being named differently
rank_col = df.columns[0]
df = df[~df[rank_col].astype(str).str.contains('Page size|items in', na=False)].reset_index(drop=True)

logging.info(f"Columns detected: {df.columns.tolist()}")

# Select desired columns — fill any missing ones with None rather than crashing
DESIRED = ['Name', 'Team', 'PA', 'BB%', 'K%', 'BB/K', 'AVG', 'OBP', 'SLG', 'OPS',
           'ISO', 'BABIP', 'wRC', 'wRAA', 'wOBA', 'wRC+']

for col in DESIRED:
    if col not in df.columns:
        logging.warning(f"  Column '{col}' not found in this split — filling with None")
        df[col] = None

df = df[DESIRED].copy()

df.rename(columns={
    'Name': 'player', 'Team': 'team', 'PA': 'pa',
    'BB%': 'bb_pct', 'K%': 'k_pct', 'BB/K': 'bb_k',
    'AVG': 'avg', 'OBP': 'obp', 'SLG': 'slg', 'OPS': 'ops',
    'ISO': 'iso', 'BABIP': 'babip',
    'wRC': 'wrc', 'wRAA': 'wraa', 'wOBA': 'woba', 'wRC+': 'wrc_plus'
}, inplace=True)

# Strip % and convert — coerce bad values (e.g. "-") to NaN instead of crashing
for col in ['bb_pct', 'k_pct']:
    df[col] = pd.to_numeric(
        df[col].astype(str).str.replace('%', '', regex=False),
        errors='coerce'
    ) / 100

# Coerce all remaining numeric columns so "-" or blanks don't crash Supabase
for col in ['pa', 'bb_k', 'avg', 'obp', 'slg', 'ops', 'iso', 'babip', 'wrc', 'wraa', 'woba', 'wrc_plus']:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Convert integer columns (pa, wrc, wraa) — use Int64 to safely allow NaN
df['pa']   = df['pa'].astype('Int64')
df['wrc']  = df['wrc'].astype('Int64')
df['wraa'] = df['wraa'].round(1)  # keep as float; FanGraphs shows decimals here

df['split_type']  = SPLIT_NAME
df['scrape_date'] = datetime.now().date().isoformat()  # string so Supabase can serialize it

# Drop rows where player name is missing (can happen with pagination artifacts)
df = df.dropna(subset=['player']).reset_index(drop=True)

records = df.to_dict(orient="records")

# Int64 NaN-safe conversion for JSON serialization
for record in records:
    for k, v in record.items():
        if pd.isna(v):
            record[k] = None

# Delete all existing rows for this split before inserting fresh data
supabase.table("fangraphs_advanced_batting").delete().eq(
    "split_type", SPLIT_NAME
).execute()
logging.info(f"🗑️  Cleared old rows for '{SPLIT_NAME}'")

supabase.table("fangraphs_advanced_batting").upsert(
    records, on_conflict="player,team,split_type,scrape_date"
).execute()

logging.info(f"✅ {len(df):,} rows upserted for {SPLIT_NAME}")
time.sleep(4)
