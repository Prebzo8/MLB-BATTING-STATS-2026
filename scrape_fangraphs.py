import os
import pandas as pd
import requests
from datetime import datetime
from supabase import create_client, Client
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# === SUPABASE SETUP ===
supabase: Client = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"]
)

# === YOUR 5 URLS (Advanced stats) ===
urls = {
    "No Splits": "https://www.fangraphs.com/leaders-legacy.aspx?pos=np&stats=bat&lg=all&qual=5&type=1&season=2026&month=0&season1=2026&ind=0&team=0&rost=0&age=0&filter=&players=0&startdate=2026-01-01&enddate=2026-12-31&sort=3,d&page=1_2000",
    "vs R":      "https://www.fangraphs.com/leaders-legacy.aspx?pos=np&stats=bat&lg=all&qual=5&type=1&season=2026&month=14&season1=2026&ind=0&team=0&rost=0&age=0&filter=&players=0&page=1_2000",
    "vs L":      "https://www.fangraphs.com/leaders-legacy.aspx?pos=np&stats=bat&lg=all&qual=5&type=1&season=2026&month=13&season1=2026&ind=0&team=0&rost=0&age=0&filter=&players=0&page=1_2000",
    "Home":      "https://www.fangraphs.com/leaders-legacy.aspx?pos=np&stats=bat&lg=all&qual=5&type=1&season=2026&month=15&season1=2026&ind=0&team=0&rost=0&age=0&filter=&players=0&page=1_2000",
    "Away":      "https://www.fangraphs.com/leaders-legacy.aspx?pos=np&stats=bat&lg=all&qual=5&type=1&season=2026&month=16&season1=2026&ind=0&team=0&rost=0&age=0&filter=&players=0&page=1_2000"
}

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

for split_name, url in urls.items():
    logging.info(f"📥 Scraping {split_name}...")
    
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    df = pd.read_html(resp.text)[0]
    
    # Keep only your columns + rename to snake_case
    df = df[['Player', 'Team', 'PA', 'BB%', 'K%', 'BB/K', 'AVG', 'OBP', 'SLG', 'OPS',
             'ISO', 'BABIP', 'wRC', 'wRAA', 'wOBA', 'wRC+']].copy()
    
    df.rename(columns={
        'Player': 'player', 'Team': 'team', 'PA': 'pa',
        'BB%': 'bb_pct', 'K%': 'k_pct', 'BB/K': 'bb_k',
        'AVG': 'avg', 'OBP': 'obp', 'SLG': 'slg', 'OPS': 'ops',
        'ISO': 'iso', 'BABIP': 'babip',
        'wRC': 'wrc', 'wRAA': 'wraa', 'wOBA': 'woba', 'wRC+': 'wrc_plus'
    }, inplace=True)
    
    # Clean percentages (e.g. "12.3%" → 0.123)
    for col in ['bb_pct', 'k_pct']:
        df[col] = df[col].astype(str).str.replace('%', '', regex=False).astype(float) / 100
    
    # Add metadata
    df['split_type'] = split_name
    df['scrape_date'] = datetime.now().date()
    
    # Upload to Supabase (safe daily upsert)
    records = df.to_dict(orient="records")
    supabase.table("fangraphs_advanced_batting").upsert(
        records,
        on_conflict="player,team,split_type,scrape_date"
    ).execute()
    
    logging.info(f"✅ {len(df):,} rows upserted for {split_name}")

logging.info("🎉 Daily scrape & Supabase upload finished!")
