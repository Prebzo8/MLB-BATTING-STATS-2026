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

# EXACT column order you originally asked for + PA guaranteed
cols = ['Season', 'Name', 'Team', 'PA', 'BB%', 'K%', 'BB/K',
        'AVG', 'OBP', 'SLG', 'OPS', 'ISO', 'BABIP',
        'wRC', 'wRAA', 'wOBA', 'wRC+']

rename_map = {
    'IDfg': 'idfg', 'Season': 'season', 'Name': 'name', 'Team': 'tm',
    'PA': 'pa', 'BB%': 'bb_percent', 'K%': 'k_percent', 'BB/K': 'bb_k',
    'AVG': 'avg', 'OBP': 'obp', 'SLG': 'slg', 'OPS': 'ops',
    'ISO': 'iso', 'BABIP': 'babip',
    'wRC': 'wrc', 'wRAA': 'wraa', 'wOBA': 'woba', 'wRC+': 'wrc_plus'
}

def update_table(table_name, df_raw):
    if len(df_raw) == 0:
        print(f"   ⚠️ No data for {table_name}")
        return
    
    df = df_raw[cols].copy()                    # ← PA is explicitly here
    df = df.rename(columns=rename_map)
    print(f"   → {len(df)} rows | PA column included for {table_name}")
    
    # Clear + insert
    supabase.table(table_name).delete().neq('idfg', -1).execute()
    supabase.table(table_name).insert(df.to_dict(orient='records')).execute()
    print(f"   ✅ {table_name} updated!")

# ==================== FETCH ALL 5 DATASETS ====================
print("Fetching data from FanGraphs...")

# 1. Overall (your original qual=10)
data_overall = batting_stats(2026, qual=10)
update_table('batting_stats_2026', data_overall)

# 2-5. Splits (qual=0 so we get data early in the season)
def fetch_split(split_code, table_name):
    url = f"https://www.fangraphs.com/leaders/major-league?pos=all&stats=bat&lg=all&qual=0&type=8&season=2026&month={split_code}&season1=2026&ind=0&team=0,ts&rost=0&age=0&filter=&players=0"
    try:
        tables = pd.read_html(url)
        for t in tables:
            if 'Name' in t.columns and 'PA' in t.columns:   # ← PA column check
                update_table(table_name, t)
                return
        print(f"   ⚠️ Could not find PA column for {table_name}")
    except Exception as e:
        print(f"   ❌ Failed to fetch {table_name}: {e}")

fetch_split("13", "batting_stats_2026_vs_lhp")   # vs LHP
fetch_split("14", "batting_stats_2026_vs_rhp")   # vs RHP
fetch_split("15", "batting_stats_2026_home")     # Home
fetch_split("16", "batting_stats_2026_away")     # Away

print("🎉 All 5 tables updated with PA column!")

# ============== TELEGRAM NOTIFICATION ==============
print("Sending Telegram notification...")
try:
    token = os.environ["TELEGRAM_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    
    message = f"""✅ **2026 MLB Batting Stats Updated!**

• Overall: {len(data_overall)} players (min 10 PA)
• vs LHP / vs RHP / Home / Away: min 1 PA (early season)

✅ PA column included in ALL 5 tables"""

    requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
    )
    print("✅ Telegram message sent!")
except Exception as e:
    print(f"⚠️ Telegram failed: {e}")
