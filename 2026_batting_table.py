from pybaseball import batting_stats
import pandas as pd
from supabase import create_client, Client
import os

# Connect to Supabase
supabase: Client = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_SERVICE_ROLE_KEY"]
)

# Pull 2026 stats (min 10 PA)
data = batting_stats(2026, qual=10)

# Exact columns you wanted
cols = ['IDfg', 'Season', 'Name', 'Team', 'PA', 'BB%', 'K%', 'BB/K',
        'AVG', 'OBP', 'SLG', 'OPS', 'ISO', 'BABIP',
        'wRC', 'wRAA', 'wOBA', 'wRC+']

df = data[cols].copy()
df = df.rename(columns={'Team': 'Tm', 'IDfg': 'idfg'})

# Keep raw numeric values (best for database)
# No % formatting or string conversion here

# Convert to list of dicts for Supabase
records = df.to_dict(orient='records')

# Upsert into Supabase (updates existing rows, inserts new ones)
result = supabase.table('batting_stats_2026').upsert(
    records,
    on_conflict='idfg'
).execute()

print(f"✅ Updated {len(records)} player rows in Supabase!")