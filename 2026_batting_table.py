from pybaseball import batting_stats
import pandas as pd
from supabase import create_client, Client
import os

print("Starting 2026 batting stats (No Splits) update...")

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

# Pull 2026 stats (min 10 PA)
print("Fetching data from FanGraphs...")
data = batting_stats(2026, qual=10)

cols = ['IDfg', 'Season', 'Name', 'Team', 'PA', 'BB%', 'K%', 'BB/K',
        'AVG', 'OBP', 'SLG', 'OPS', 'ISO', 'BABIP',
        'wRC', 'wRAA', 'wOBA', 'wRC+']

df = data[cols].copy()

# Rename columns to EXACTLY match your Supabase table
df = df.rename(columns={
    'IDfg': 'idfg',
    'Season': 'season',
    'Name': 'name',
    'Team': 'tm',
    'PA': 'pa',
    'BB%': 'bb_percent',
    'K%': 'k_percent',
    'BB/K': 'bb_k',
    'AVG': 'avg',
    'OBP': 'obp',
    'SLG': 'slg',
    'OPS': 'ops',
    'ISO': 'iso',
    'BABIP': 'babip',
    'wRC': 'wrc',
    'wRAA': 'wraa',
    'wOBA': 'woba',
    'wRC+': 'wrc_plus'
})

print(f"✅ Fetched and prepared {len(df)} players")

# Clear old data and insert fresh data
print("Clearing old data from Supabase...")
supabase.table('batting_stats_nosplits_2026').delete().neq('idfg', -1).execute()

print("Inserting new data...")
result = supabase.table('batting_stats_nosplits_2026').insert(df.to_dict(orient='records')).execute()

print(f"🎉 Successfully loaded {len(df)} rows into Supabase!")
