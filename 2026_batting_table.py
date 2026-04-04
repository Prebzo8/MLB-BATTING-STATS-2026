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

# Columns (IDfg required for PK + PA as requested)
pull_cols = ['IDfg', 'Season', 'Name', 'Team', 'PA', 'BB%', 'K%', 'BB/K',
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
    df = df_raw[pull_cols].copy()
    df = df.rename(columns=rename_map)
    print(f"   → {len(df)} rows | PA included → {table_name}")
    
    supabase.table(table_name).delete().neq('idfg', -1).execute()
    supabase.table(table_name).insert(df.to_dict(orient='records')).execute()
    print(f"   ✅ {table_name} updated!")

# ==================== OVERALL ====================
print("Fetching Overall...")
data_overall = batting_stats(2026, qual=10)
update_table('batting_stats_2026', data_overall)

# ==================== SPLITS (improved scraping) ====================
def fetch_split(split_code, table_name):
    url = f"https://www.fangraphs.com/leaders/major-league?pos=all&stats=bat&lg=all&qual=0&type=8&season=2026&month={split_code}&season1=2026&ind=0&team=0,ts&rost=0&age=0&filter=&players=0"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    }
    print(f"   Fetching {table_name} from {url}")
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        tables = pd.read_html(response.text)
        print(f"   Found {len(tables)} tables on page")
        
        for i, t in enumerate(tables):
            cols = list(t.columns)
            print(f"     Table {i} columns: {cols[:8]}...")   # debug
            if 'Name' in t.columns and 'PA' in t.columns:
                print(f"     ✅ Using Table {i} for {table_name}")
                update_table(table_name, t)
                return
        print(f"   ❌ No suitable table found for {table_name}")
    except Exception as e:
        print(f"   ❌ Failed to fetch {table_name}: {e}")

fetch_split("13", "batting_stats_2026_vs_lhp")
fetch_split("14", "batting_stats_2026_vs_rhp")
fetch_split("15", "batting_stats_2026_home")
fetch_split("16", "batting_stats_2026_away")

print("🎉 All 5 tables processed!")

# ============== TELEGRAM NOTIFICATION ==============
print("Sending Telegram notification...")
try:
    token = os.environ["TELEGRAM_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    
    message = f"""✅ **2026 MLB Batting Stats Updated!**

• Overall: {len(data_overall)} players (min 10 PA)
• Splits (vs LHP / vs RHP / Home / Away): check logs for row counts

PA column is in all tables!"""

    requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
    )
    print("✅ Telegram message sent!")
except Exception as e:
    print(f"⚠️ Telegram failed: {e}")
