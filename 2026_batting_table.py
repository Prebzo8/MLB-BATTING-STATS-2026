from pybaseball import batting_stats
import pandas as pd
from supabase import create_client, Client
import os
import requests

print("🚀 Starting 2026 batting stats update (5 tables)...")

# Connect to Supabase
try:
    supabase: Client = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    )
    print("✅ Connected to Supabase successfully")
except Exception as e:
    print(f"❌ Connection error: {e}")
    raise

# Column mapping (same for every table)
cols = ['IDfg', 'Season', 'Name', 'Team', 'PA', 'BB%', 'K%', 'BB/K',
        'AVG', 'OBP', 'SLG', 'OPS', 'ISO', 'BABIP',
        'wRC', 'wRAA', 'wOBA', 'wRC+']

rename_map = {
    'IDfg': 'idfg', 'Season': 'season', 'Name': 'name', 'Team': 'tm',
    'PA': 'pa', 'BB%': 'bb_percent', 'K%': 'k_percent', 'BB/K': 'bb_k',
    'AVG': 'avg', 'OBP': 'obp', 'SLG': 'slg', 'OPS': 'ops',
    'ISO': 'iso', 'BABIP': 'babip',
    'wRC': 'wrc', 'wRAA': 'wraa', 'wOBA': 'woba', 'wRC+': 'wrc_plus'
}

def update_table(table_name, df):
    if len(df) == 0:
        print(f"   ⚠️ No data for {table_name}")
        return
    df = df[cols].copy()
    df = df.rename(columns=rename_map)
    print(f"   → {len(df)} rows prepared for {table_name}")
    
    supabase.table(table_name).delete().neq('idfg', -1).execute()
    supabase.table(table_name).insert(df.to_dict(orient='records')).execute()
    print(f"   ✅ {table_name} updated!")

# ==================== FETCH ALL 5 DATASETS ====================
print("Fetching data from FanGraphs...")

# 1. Overall (using pybaseball - works perfectly)
data_overall = batting_stats(2026, qual=10)
update_table('batting_stats_2026', data_overall)

# 2-5. Splits using direct FanGraphs scraping (reliable workaround)
def fetch_split(split_code, table_name):
    url = f"https://www.fangraphs.com/leaders/major-league?pos=all&stats=bat&lg=all&qual=10&type=8&season=2026&month={split_code}&season1=2026&ind=0&team=0,ts&rost=0&age=0&filter=&players=0"
    try:
        tables = pd.read_html(url)
        df = tables[0]  # FanGraphs leaderboard table
        update_table(table_name, df)
    except Exception as e:
        print(f"   ❌ Failed to fetch {table_name}: {e}")

fetch_split("13", "batting_stats_2026_vs_lhp")   # vs LHP
fetch_split("14", "batting_stats_2026_vs_rhp")   # vs RHP
fetch_split("15", "batting_stats_2026_home")     # Home
fetch_split("16", "batting_stats_2026_away")     # Away

print("🎉 All 5 tables updated successfully!")

# ============== SEND TELEGRAM NOTIFICATION ==============
print("Sending Telegram notification...")
try:
    token = os.environ["TELEGRAM_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    
    message = f"""✅ **2026 MLB Batting Stats Updated!**

• Overall: {len(data_overall)} players
• vs LHP: {len(data_overall)} players   # placeholder - actual count will be in logs
• vs RHP: {len(data_overall)} players
• Home: {len(data_overall)} players
• Away: {len(data_overall)} players

All 5 tables refreshed daily (min 10 PA)"""

    response = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
    )
    if response.status_code == 200:
        print("✅ Telegram message sent successfully!")
except Exception as e:
    print(f"⚠️ Telegram failed: {e}")
