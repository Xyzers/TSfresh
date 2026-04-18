import sqlite3
import pandas as pd

def analyze():
    conn = sqlite3.connect('maintenance_pilote_400j.db')
    
    # 2) Raw Stats via SQL
    print('Raw Stats...')
    stats_raw = {}
    for c in ['intensite', 'vibration', 'debit_entree', 'vitesse']:
        n_null = conn.execute(f'SELECT COUNT(*) FROM raw_historian WHERE {c} IS NULL').fetchone()[0]
        stats_raw[c] = n_null
    
    total = 48694833
    print(f'Raw: {total} lines')
    for c, n in stats_raw.items():
        print(f' - {c}: {n} null ({n/total:.2%})')

    # 3) Window Analysis
    print('\nWindowing...')
    sig = pd.read_sql('SELECT timestamp, duree_sec, i_nervosite FROM signatures_sante', conn)
    results = []
    
    for _, row in sig.iterrows():
        t0 = row['timestamp']
        dur = max(5, row['duree_sec'] if pd.notna(row['duree_sec']) else 5)
        
        # Use simple string compare for ISO dates in SQLite
        query = f"SELECT intensite FROM raw_historian WHERE DateTime >= '{t0}' AND DateTime <= datetime('{t0}', '+{dur} seconds')"
        win_vals = [r[0] for r in conn.execute(query).fetchall()]
        
        n_val = sum(1 for x in win_vals if x is not None)
        # diff (count non-null transitions)
        n_diff = 0
        if len(win_vals) > 1:
            valid_vals = [x for x in win_vals if x is not None]
            if len(valid_vals) > 1:
                n_diff = sum(1 for i in range(len(valid_vals)-1) if valid_vals[i] != valid_vals[i+1])
        
        results.append({'n_val': n_val, 'n_diff': n_diff, 'is_nan': pd.isna(row['i_nervosite'])})

    res = pd.DataFrame(results)
    print('\nDist n_val:')
    print(res['n_val'].describe(percentiles=[.1, .5, .9]))
    print(f'Events <= 1 diff: {(res.n_diff <= 1).sum()} / {len(sig)}')
    
    print('\nCorrelation i_nervosite NaN vs non-NaN (n_val stats):')
    print(res.groupby('is_nan')['n_val'].agg(['mean', 'median', 'count']))
    conn.close()

analyze()
