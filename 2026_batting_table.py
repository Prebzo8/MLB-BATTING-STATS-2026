from pybaseball import batting_stats
import pandas as pd
from supabase import create_client, Client
import os

print("Starting 2026 batting stats update...")

# Connect to Supabase with better error handling
try:
    supabase: Client = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    )
    print("✅ Connected to Supabase successfully")
except KeyError as e:
    print(f"❌ Missing environment variable: {e}")
    print("Make sure SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are set in GitHub Secrets")
    raise
except Exception as e:
    print(f"❌ Supabase connection failed: {e}")
    raise

# Pull 2026 stats (min 10 PA)
print("Fetching data from FanGraphs...")
data = batting_stats(2026, qual=10)

cols = ['IDfg', 'Season', 'Name', 'Team', 'PA', 'BB%', 'K%', 'BB/K',
        'AVG', 'OBP', 'SLG', 'OPS', 'ISO', 'BABIP',
        'wRC', 'wRAA', 'wOBA', 'wRC+']

df = data[cols].copy()
df = df.rename(columns={'Team': 'Tm', 'IDfg': 'idfg'})

print(f"✅ Fetched {len(df)} players with ≥10 PA")

# Delete old data and insert fresh data (most reliable method)
print("Clearing old data from Supabase...")
supabase.table('batting_stats_2026').delete().neq('idfg', -1).execute()  # deletes everything

print("Inserting new data...")
records = df.to_dict(orient='records')
result = supabase.table('batting_stats_2026').insert(records).execute()

print(f"🎉 Successfully loaded {len(records)} rows into Supabase!")
