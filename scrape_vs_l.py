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

URL = "https://www.fangraphs.com/leaders-legacy.aspx?pos=np&stats=bat&lg=all&qual=5&type=1&season=2026&month=13&season1=2026&ind=0&team=0&rost=0&age=0&filter=&players=0&page=1_2000"
SPLIT_NAME = "vs L"

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

df = df[['Player', 'Team', 'PA', 'BB%', 'K%', 'BB/K', 'AVG', 'OBP', 'SLG', 'OPS',
         'ISO', 'BABIP', 'wRC', 'wRAA', 'wOBA', 'wRC+']].copy()

df.rename(columns={
    'Player': 'player', 'Team': 'team', 'PA': 'pa',
    'BB%': 'bb_pct', 'K%': 'k_pct', 'BB/K': 'bb_k',
    'AVG': 'avg', 'OBP': 'obp', 'SLG': 'slg', 'OPS': 'ops',
    'ISO': 'iso', 'BABIP': 'babip',
    'wRC': 'wrc', 'wRAA': 'wraa', 'wOBA': 'woba', 'wRC+': 'wrc_plus'
}, inplace=True)

for col in ['bb_pct', 'k_pct']:
    df[col] = df[col].astype(str).str.replace('%', '', regex=False).astype(float) / 100

df['split_type'] = SPLIT_NAME
df['scrape_date'] = datetime.now().date()

records = df.to_dict(orient="records")
supabase.table("fangraphs_advanced_batting").upsert(
    records, on_conflict="player,team,split_type,scrape_date"
).execute()

logging.info(f"✅ {len(df):,} rows upserted for {SPLIT_NAME}")
time.sleep(4)
