import sqlite3


def assurer_indexation_db(db_path):
    """Crée un index sur DateTime pour accélérer les requêtes massives (Indispensable pour 4.3 Go)"""
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        logging.info(" Vérification de l'indexation de la base de données...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_datetime ON raw_historian(DateTime)")
        conn.commit()
        logging.info(" Indexation terminée ou déjà présente.")
    except Exception as e:
        logging.error(f" Erreur lors de l'indexation : {e}")
    finally:
        conn.close()


import pandas as pd
import numpy as np
import logging
import configparser
from datetime import timedelta

# Configuration des logs pour voir ce qui se passe dans la console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def analyser_signatures_haute_fidelite(db_path="maintenance_pilote_400j.db", config_path="Config.ini"):
    logging.info(" Démarrage de l'analyse des signatures...")

    # --- 0. OPTIMISATION DATABASE ---
    # Création de l'index sur DateTime (opération unique pour accélérer les tris sur 4.3 Go)
    assurer_indexation_db(db_path)
    
    # --- 1. CHARGEMENT DE LA CONFIGURATION ---
    config = configparser.ConfigParser()
    config.read(config_path)
    
    try:
        saut_declencheur = config.getfloat('Thresholds', 'trigger_jump', fallback=16.0)
        marge_hysteresis = config.getfloat('Thresholds', 'hysteresis_margin', fallback=6.0)
        seuil_relachement = saut_declencheur - marge_hysteresis
        
        seuil_intensite = config.getfloat('Thresholds', 'intensity_peak', fallback=70.0)
        # Nouveau paramètre pour votre POINT BLEU
        fenetre_bleue_sec = config.getfloat('Thresholds', 'blue_peak_window', fallback=4.0)
        
        seuil_vitesse_min = config.getfloat('Thresholds', 'min_speed', fallback=5600.0)
        seuil_debit_max = config.getfloat('Thresholds', 'flow_cutoff', fallback=1.5)
        duree_min = config.getint('Thresholds', 'min_duration_sec', fallback=3)
        max_duration_sec = config.getint('Thresholds', 'max_duration_sec', fallback=480)

        # Paramètres heuristiques de détection des lavages (NEP)
        nep_burst_window_h = config.getfloat('Thresholds', 'nep_burst_window_h', fallback=3.0)
        nep_burst_min_events = config.getint('Thresholds', 'nep_burst_min_events', fallback=7)
        
        logging.info(f" Config : Déclenchement > {saut_declencheur}A | Fenêtre Point Bleu : {fenetre_bleue_sec}s")
        logging.info(f" Config NEP : Rafale > {nep_burst_min_events} événements en {nep_burst_window_h}h => exclusion")
    except Exception as e:
        logging.error(f" Erreur de lecture Config.ini : {e}")
        return

    # --- 2. CONNEXION ET CHARGEMENT ---
    conn = sqlite3.connect(db_path)
    logging.info(" Lecture de la base de données (raw_historian)...")
    
    # On charge les données
    query = "SELECT DateTime, intensite, vibration, debit_entree, vitesse FROM raw_historian ORDER BY DateTime"
    df = pd.read_sql(query, conn)
    
    if df.empty:
        logging.warning(" La table raw_historian est vide !")
        conn.close()
        return

    # Correction du format de date Mixed
    df['DateTime'] = pd.to_datetime(df['DateTime'], format='mixed')

    # --- 2.5 ROBUSTESSE MODE DELTA ---
    # Pontage des NaNs (jusqu'à 30 lignes) pour assurer la compatibilité entre versions de Pandas
    cols_a_boucher = ['intensite', 'vibration', 'debit_entree', 'vitesse']
    df[cols_a_boucher] = df[cols_a_boucher].ffill(limit=30)

    # --- 3. DETECTION PAR HYSTERESIS ---
    logging.info(" Détection des événements en cours...")
    df['courant_de_repos'] = df.rolling('60s', on='DateTime')['intensite'].median()
    df['saut_intensite'] = df['intensite'] - df['courant_de_repos']
    
    # Marquage des zones actives
    df['is_event'] = df['saut_intensite'] > saut_declencheur
    df['is_event_smoothed'] = df.rolling('30s', on='DateTime')['is_event'].max().fillna(0).astype(bool)
    df['event_id'] = (df['is_event_smoothed'] != df['is_event_smoothed'].shift()).cumsum()
    
    events = df[df['is_event_smoothed']]
    signatures = []
    
    logging.info(f" Analyse de {events['event_id'].nunique()} zones potentielles...")

    for eid, group in events.groupby('event_id'):
        # Détermination précise de t0 et t_end par hystérésis
        p_start = group[group['saut_intensite'] > saut_declencheur]
        if p_start.empty: continue
        t0 = p_start['DateTime'].min()
        
        p_end = group[group['saut_intensite'] > seuil_relachement]
        t_end = p_end['DateTime'].max()
        
        duree = (t_end - t0).total_seconds()

        # --- FILTRES DE VALIDATION ---
        if duree < duree_min: continue
        # Proposition A : Application du filtre max_duration_sec pour esquiver 
        # les démarrages prolongés de machine (Faux Positifs).
        if duree > max_duration_sec: continue
        if group['vitesse'].mean() < seuil_vitesse_min: continue
        if group['debit_entree'].min() > seuil_debit_max: continue

        # --- 4. CALCUL DU POINT BLEU (PIC INITIAL) ---
        # Proposition B : Fenêtre glissante adaptative pour éviter les Faux Négatifs
        # si la montée en charge mécanique prend plus de temps (usure du tiroir).
        # On étend la fenêtre jusqu'à la première redescente franche (>2A) ou on garde 4s.
        t_limite_dynamique = t0 + pd.Timedelta(seconds=fenetre_bleue_sec)
        chute_franche_mask = (group['DateTime'] > t0) & (group['intensite'].diff() < -2.0)
        
        if chute_franche_mask.any():
            t_premiere_chute = group[chute_franche_mask]['DateTime'].min()
            t_limite_bleue = max(t_limite_dynamique, t_premiere_chute)
        else:
            t_limite_bleue = t_limite_dynamique

        zone_attaque = group[group['DateTime'] <= t_limite_bleue]
        
        if zone_attaque.empty: continue
        
        idx_point_bleu = zone_attaque['intensite'].idxmax()
        t_max_int = zone_attaque.loc[idx_point_bleu, 'DateTime']
        val_max_int = zone_attaque.loc[idx_point_bleu, 'intensite']

        # Validation : si le pic bleu est inférieur au seuil process, on rejette
        if val_max_int < seuil_intensite:
            continue

        # --- 5. CALCUL VIBRATION ET LATENCE ---
        group_calc = group.copy()
        group_calc['v_smooth'] = group_calc.rolling('1s', on='DateTime')['vibration'].mean()
        
        # Pic vibration restreint
        # Proposition C : Borner la recherche du maximum vibratoire autour du 
        # pic de coup de bélier pour éviter de biaiser la lecture par des chocs parasites.
        t_limite_vib = min(t_end, t_max_int + pd.Timedelta(seconds=15))
        mask_v = (group_calc['DateTime'] >= t_max_int) & (group_calc['DateTime'] <= t_limite_vib)
        
        # Sécurité: fallback si le mask strict est exceptionnellement vide
        if not mask_v.any(): mask_v = (group_calc['DateTime'] >= t0) & (group_calc['DateTime'] <= t_end)
        
        idx_max_vib = group_calc[mask_v]['v_smooth'].idxmax()
        t_max_vib = group_calc.loc[idx_max_vib, 'DateTime']
        
        # L'Historian étant 'Delta', la vibration peut être NaN sur l'exacte milliseconde 
        # où v_smooth (la moyenne mobile) atteint son max. On utilise ffill pour lire la dernière vraie valeur.
        v_peak_brut = group_calc['vibration'].ffill().loc[idx_max_vib]

        # Proposition D : Rééchantillonnage temporel isochrone pour i_nervosite
        # Empêche que l'Historian 'Delta asynchrone' ne fausse le calcul de la dérivée absolue
        mask_nerv = (group['DateTime'] >= t0) & (group['DateTime'] <= t_end)
        group_nerv = group[mask_nerv][['DateTime', 'intensite']].copy()
        group_nerv = group_nerv[~group_nerv['DateTime'].duplicated()].set_index('DateTime')
        
        if len(group_nerv) > 2:
            nervosite_calc = group_nerv.resample('500ms').interpolate(method='linear')['intensite'].diff().abs().mean()
        else:
            nervosite_calc = group[mask_nerv]['intensite'].diff().abs().mean()

        # --- NOUVEAU : CALCUL DE L'ÉNERGIE (INTÉGRALE DE PUISSANCE) ---
        # L'intégrale de l'intensité sur la durée de l'événement s'additionne très finement
        # avec une méthode des trapèzes, ce qui est quasi instantané au niveau calcul (O(N)).
        if len(group[mask_nerv]) > 1:
            y = group[mask_nerv]['intensite'].ffill().bfill().values
            x = (group[mask_nerv]['DateTime'] - t0).dt.total_seconds().values
            try:
                energie_calc = np.trapezoid(y, x)
            except AttributeError:
                energie_calc = np.trapz(y, x)
        else:
            energie_calc = 0.0

        # --- 6. ARCHIVAGE DE LA SIGNATURE ---
        signatures.append({
            'event_id': f"{t0.strftime('%Y%m%d_%H%M%S')}",
            'timestamp': t0,
            'v_peak': v_peak_brut,
            'i_nervosite': nervosite_calc,
            'energie_debourbage': energie_calc,
            't_elec_ms': (t_max_int - t0).total_seconds() * 1000,
            't_latence_ms': (t_max_vib - t_max_int).total_seconds() * 1000,
            't_relax_ms': max(0, (t_end - t_max_vib).total_seconds() * 1000),
            'intensite_max': val_max_int, # Le Point Bleu
            'duree_sec': duree
        })

    # --- 7. FILTRE NEP (Heuristique de densité temporelle) ---
    # Principe : pendant un lavage chimique (NEP), les débourbages partiels (DP) à fort débit
    # sont détectés comme de faux débourbages totaux. Cette rafale génère une densité anormale
    # d'événements (>> 5) sur une courte fenêtre. En production normale, un opérateur ne
    # réalise jamais plus de 5 DT sur 40 minutes, et jamais plus de 6 sur une journée entière.
    df_sig = pd.DataFrame(signatures)
    if not df_sig.empty:
        df_sig = df_sig.sort_values('timestamp').reset_index(drop=True)
        
        nep_window = pd.Timedelta(hours=nep_burst_window_h)
        
        # Compter pour chaque événement combien d'autres tombent dans la fenêtre glissante
        def count_in_window(ts, timestamps, window):
            return ((timestamps >= ts) & (timestamps <= ts + window)).sum()
        
        ts_series = df_sig['timestamp']
        df_sig['nep_count'] = ts_series.apply(lambda ts: count_in_window(ts, ts_series, nep_window))
        
        # Marquer les rafales NEP (l'événement lui-même est compté dans nep_count, d'où >= seuil)
        masque_nep = df_sig['nep_count'] >= nep_burst_min_events
        n_exclus = masque_nep.sum()
        
        if n_exclus > 0:
            logging.warning(f" Filtre NEP : {n_exclus} événements exclus (rafale de lavage détectée).")
        else:
            logging.info(" Filtre NEP : Aucune rafale de lavage détectée.")
        
        # Supprimer la colonne de travail et les comptes de lavage
        df_sig = df_sig[~masque_nep].drop(columns=['nep_count'])

    # --- 8. SAUVEGARDE FINALE ---
    if not df_sig.empty:
        df_sig.to_sql('signatures_sante', conn, if_exists='replace', index=False)
        logging.info(f" Succès : {len(df_sig)} signatures enregistrées dans 'signatures_sante'.")
    else:
        logging.warning(" Aucune signature n'a survécu aux filtres (vérifiez le seuil de 70A).")
        
    conn.close()


if __name__ == "__main__":
    analyser_signatures_haute_fidelite()
