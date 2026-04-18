import sqlite3
import pandas as pd
import numpy as np

try:
    conn = sqlite3.connect('maintenance_pilote_400j.db')
    raw = pd.read_sql('SELECT * FROM raw_historian', conn)
    sig = pd.read_sql('SELECT * FROM signatures_sante', conn)
    conn.close()

    raw['timestamp'] = pd.to_datetime(raw['timestamp'])
    sig['timestamp'] = pd.to_datetime(sig['timestamp'])

    print(f'Raw: {len(raw)} lines')
    for c in ['intensite', 'vibration', 'debit_entree', 'vitesse']:
        n = raw[c].isna().sum()
        print(f'{c}: {n} null ({n/len(raw):.1%})')

    results = []
    raw = raw.sort_values('timestamp')
    for _, row in sig.iterrows():
        t0 = row['timestamp']
        dur = max(5, row.get('duree_sec', 5))
        window = raw[(raw.timestamp >= t0) & (raw.timestamp <= t0 + pd.Timedelta(seconds=dur))]
        n_val = window['intensite'].notna().sum()
        n_diff = window['intensite'].diff().dropna().size
        results.append({'n_val': n_val, 'n_diff': n_diff, 'is_nan': pd.isna(row['i_nervosite'])})

    res = pd.DataFrame(results)
    print('\nDistribution n_val (intensite):')
    print(res['n_val'].describe(percentiles=[.1, .5, .9]))
    print(f"\nEvents with <= 1 diff: {(res['n_diff'] <= 1).sum()} / {len(sig)}")

    print('\nStats n_val by i_nervosite is NaN:')
    print(res.groupby('is_nan')['n_val'].agg(['mean', 'median', 'count']))
except Exception as e:
    print(f'Error: {e}')
