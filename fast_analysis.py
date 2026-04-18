import sqlite3
import pandas as pd
import sys

def analyze():
    conn = sqlite3.connect('maintenance_pilote_400j.db')
    print('Reading raw...')
    # Use chunking to avoid memory issues with 48M lines
    null_counts = {'intensite':0, 'vibration':0, 'debit_entree':0, 'vitesse':0}
    for chunk in pd.read_sql('SELECT DateTime, intensite, vibration, debit_entree, vitesse FROM raw_historian', conn, chunksize=1_000_000):
        for c in null_counts:
            null_counts[c] += chunk[c].isna().sum()
    
    total_raw = 48694833
    print(f'Total Raw: {total_raw}')
    for c, n in null_counts.items():
        print(f'{c}: {n} null ({n/total_raw:.1%})')

    print('Reading signatures...')
    sig = pd.read_sql('SELECT timestamp, duree_sec, i_nervosite FROM signatures_sante', conn)
    sig['timestamp'] = pd.to_datetime(sig['timestamp'])

    results = []
    print('Processing windows...')
    for _, row in sig.iterrows():
        t0 = row['timestamp']
        dur = max(5, row['duree_sec'] if pd.notna(row['duree_sec']) else 5)
        # Fetch only what's needed for the window
        t_end = t0 + pd.Timedelta(seconds=dur)
        window = pd.read_sql(f"SELECT intensite FROM raw_historian WHERE DateTime BETWEEN '{t0}' AND '{t_end}'", conn)
        n_val = window['intensite'].notna().sum()
        n_diff = window['intensite'].diff().dropna().size
        results.append({'n_val': n_val, 'n_diff': n_diff, 'is_nan': pd.isna(row['i_nervosite'])})

    res = pd.DataFrame(results)
    print('\nDist n_val (intensite):')
    print(res['n_val'].describe(percentiles=[.1, .5, .9]))
    print(f'Events <= 1 diff: {(res.n_diff <= 1).sum()} / {len(sig)}')
    print('\nStats by i_nervosite is NaN:')
    print(res.groupby('is_nan')['n_val'].agg(['mean', 'median', 'count']))
    conn.close()

analyze()
