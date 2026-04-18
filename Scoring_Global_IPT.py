import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import configparser
import logging
import numpy as np
from datetime import timedelta

# Configuration pour améliorer le rendu
plt.rcParams['figure.dpi'] = 100
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['axes.facecolor'] = 'white'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def charger_config_complete(config_path="Config.ini"):
    config = configparser.ConfigParser()
    config.read(config_path)
    
    def lire_dates(section, cle):
        chaine = config.get(section, cle, fallback="")
        if not chaine.strip(): return []
        return pd.to_datetime([d.strip() for d in chaine.split(',') if d.strip()])

    params = {
        'maint_majeur': lire_dates('Maintenance', 'entretien_majeur'),
        'maint_nettoyage': lire_dates('Maintenance', 'nettoyage_assiettes'),
        'maint_nep': lire_dates('Maintenance', 'nep'),
        'maint_autres': lire_dates('Maintenance', 'autres'),
        'period_start': config.get('Period', 'start_time', fallback='').strip(),
        'period_end': config.get('Period', 'end_time', fallback='').strip(),
        
        'seuil_alerte': config.getfloat('Scoring', 'seuil_ipt_alerte', fallback=1.3),
        'seuil_critique': config.getfloat('Scoring', 'seuil_ipt_critique', fallback=1.6),
        'seuil_arret_h': config.getfloat('Thresholds', 'stop_threshold_h', fallback=23.0),
    }
    return params

def tracer_maintenance_et_arrets(ax, params, arrets_prolonges):
    label_arret = f"Arrêt (>{params['seuil_arret_h']:.0f}h)"
    for i, (debut, fin) in enumerate(arrets_prolonges):
        ax.axvspan(debut, fin, color='gray', alpha=0.08, label=label_arret if i==0 else "")
    
    for i, d in enumerate(params['maint_majeur']):
        ax.axvline(x=d, color='red', linestyle='--', lw=1.5, alpha=0.8, label='Maint. Majeure' if i==0 else "")
    for i, d in enumerate(params['maint_nettoyage']):
        ax.axvline(x=d, color='blue', linestyle='--', lw=1, alpha=0.7, label='Nettoyage' if i==0 else "")
    for i, d in enumerate(params['maint_nep']):
        ax.axvline(x=d, color='green', linestyle='--', lw=1, alpha=0.9, label='NEP' if i==0 else "")
    for i, d in enumerate(params['maint_autres']):
        ax.axvline(x=d, color='black', linestyle='-', lw=1, alpha=0.9, label='Autre' if i==0 else "")

def calculer_ipt_global(db_path="maintenance_pilote_400j.db", config_path="Config.ini"):
    params = charger_config_complete(config_path)
    conn = sqlite3.connect(db_path)
    df = pd.read_sql("SELECT * FROM signatures_sante", conn)
    conn.close()
    
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')
    df = df.sort_values('timestamp')

    period_start = pd.to_datetime(params['period_start']) if params['period_start'] else None
    period_end = pd.to_datetime(params['period_end']) if params['period_end'] else None
    if period_start is not None:
        df = df[df['timestamp'] >= period_start]
    if period_end is not None:
        df = df[df['timestamp'] <= period_end]

    if df.empty:
        logging.warning(" Aucune donnee dans la periode demandee pour le scoring.")
        return

    df = df[df['t_latence_ms'] >= 0].copy()

    features = ['v_peak', 'i_nervosite', 't_elec_ms', 't_latence_ms', 't_relax_ms']
    # i_nervosite peut etre absente sur certaines campagnes historiques :
    # on ne doit pas perdre la periode complete pour ce seul capteur.
    mandatory_features = ['v_peak', 't_elec_ms', 't_latence_ms', 't_relax_ms']
    df = df.dropna(subset=mandatory_features).copy()

    df['delta_temps'] = df['timestamp'].diff()
    seuil_delta = timedelta(hours=params['seuil_arret_h'])
    
    # Création des epochs mécaniques (basées sur les entretiens majeurs)
    maint_dates = sorted(list(params['maint_majeur']))
    if not maint_dates:
        maint_dates = [df['timestamp'].min()]
        
    df['is_new_session'] = df['delta_temps'] > seuil_delta
    # Ajouter les maintenances comme coupeures de session
    for m_date in maint_dates:
        post_idx = df[df['timestamp'] >= m_date].index[:1]
        if len(post_idx) > 0:
            df.loc[post_idx, 'is_new_session'] = True
    df['session_id'] = df['is_new_session'].cumsum()

    # --- LOCAL GOLDEN BATCH PAR ÉPOQUE MÉCANIQUE ---
    PCA_models = {}
    
    for i, m_date in enumerate(maint_dates):
        df_post_maint = df[df['timestamp'] >= m_date]
        if df_post_maint.empty:
            continue
            
        t_first_prod = df_post_maint['timestamp'].min()
        mask_calib = (df['timestamp'] >= t_first_prod) & (df['timestamp'] <= t_first_prod + timedelta(days=15))
        df_calib = df[mask_calib]
        
        # Sécurité si arrêt post-maintenance, forcer 20 events min
        if df_calib.shape[0] < 10:
            df_calib = df_post_maint.head(20)
            
        refs = {f: df_calib[f].mean() for f in features}
        stds = {f: df_calib[f].std() for f in features}
        for f in features:
            if pd.isna(stds[f]) or stds[f] == 0: stds[f] = 0.001
            
        PCA_models[m_date] = {'refs': refs, 'stds': stds}

    # Propagation des références aux époques
    epochs_bounds = [pd.to_datetime('2000-01-01')] + maint_dates + [pd.to_datetime('2100-01-01')]
    assigned_labels = [maint_dates[0]] + maint_dates
    df['epoch_idx'] = pd.cut(df['timestamp'], bins=epochs_bounds, labels=False, right=False)
    epoch_mapping = {i: assigned_labels[i] for i in range(len(assigned_labels))}
    df['epoch_maint'] = df['epoch_idx'].map(epoch_mapping)

    for f in features:
        df[f'{f}_ref'] = np.nan
        df[f'{f}_std'] = np.nan
        
    for m_date, df_epoch in df.groupby('epoch_maint'):
        if m_date not in PCA_models: continue
        model = PCA_models[m_date]
        
        for f in features:
            df.loc[df_epoch.index, f'{f}_ref'] = model['refs'][f]
            df.loc[df_epoch.index, f'{f}_std'] = model['stds'][f]

    # --- LOGIQUE IPT (Z-SCORE PIECEWISE) ---
    z_impact = (df['v_peak'] - df['v_peak_ref']) / df['v_peak_std']
    z_nerv_raw = (df['i_nervosite'] - df['i_nervosite_ref']) / df['i_nervosite_std']
    # Si i_nervosite est manquante, sa contribution devient neutre (0)
    z_nerv = np.where(np.isfinite(z_nerv_raw), z_nerv_raw, 0.0)
    
    t_total = df['t_elec_ms'] + df['t_latence_ms'] + df['t_relax_ms']
    t_total_ref = df['t_elec_ms_ref'] + df['t_latence_ms_ref'] + df['t_relax_ms_ref']
    t_total_std = df['t_elec_ms_std'] + df['t_latence_ms_std'] + df['t_relax_ms_std']
    
    z_time_raw = (t_total - t_total_ref) / t_total_std
    z_time = np.where(np.isfinite(z_time_raw), z_time_raw, 0.0)

    df['ipt_global'] = 1.0 + 0.2 * (
        0.40 * np.maximum(0, z_impact) + 
        0.30 * np.maximum(0, z_nerv) + 
        0.30 * np.maximum(0, z_time)
    )
    
    df['ipt_global'] = df.groupby('session_id')['ipt_global'].transform(lambda x: x.ewm(span=20, adjust=False).mean())

    # --- CALCUL DES TENDANCES CASSÉES PAR SESSION ---
    for col in features:
        df[f'tendance_{col}'] = df.groupby('session_id')[col].transform(lambda x: x.ewm(span=25, adjust=False).mean())
    df['tendance_ipt_global'] = df['ipt_global']

    arrets_indices = df[df['delta_temps'] > seuil_delta].index
    arrets_prolonges = []
    for idx in arrets_indices:
        t_arret = df.loc[idx, 'timestamp']
        d_arret = df.loc[idx, 'delta_temps']
        arrets_prolonges.append((t_arret - d_arret, t_arret))

    # Lignes d'affichage principal (on retire les gros arrêts connectés par un trait direct)
    df_lignes = df[~df['timestamp'].isin(df['timestamp'][arrets_indices])].copy()

    # --- SAUVEGARDE IPT EN BASE (pour Scoring_Maintenance.py) ---
    conn_ipt = sqlite3.connect(db_path)
    df[['timestamp', 'ipt_global', 'session_id']].to_sql('ipt_historique', conn_ipt, if_exists='replace', index=False)
    conn_ipt.close()
    logging.info(f" Table 'ipt_historique' ({len(df)} lignes) sauvegardée en base.")

    # === CALCUL DU RUL (Remaining Useful Life) ===
    rul_info = None
    if not df_lignes.empty:
        last_s_id = df_lignes['session_id'].iloc[-1]
        df_last_session = df_lignes[df_lignes['session_id'] == last_s_id]
        
        t_min = df_last_session['timestamp'].min()
        t_max = df_last_session['timestamp'].max()
        
        if (t_max - t_min).days >= 3 and len(df_last_session) >= 20:
            X_num = mdates.date2num(df_last_session['timestamp'])
            Y_val = df_last_session['tendance_ipt_global'].values
            
            # Regression linéaire Degré 1 : Y = aX + b
            a, b = np.polyfit(X_num, Y_val, 1)
            
            if a > 0.0001:  # Pente de dégradation mesurable
                x_critique = (params['seuil_critique'] - b) / a
                x_current = X_num[-1]
                rul_days = x_critique - x_current
                
                # Seulement si logique et n'excédant pas 5 ans
                if rul_days > 0 and rul_days < 1825:
                    y_current = a * x_current + b
                    rul_info = {
                        'a': a, 'b': b,
                        'x_current': x_current, 'x_critique': x_critique,
                        'y_current': y_current, 'y_critique': params['seuil_critique'],
                        'rul_days': rul_days,
                        'date_critique': mdates.num2date(x_critique).strftime('%d %b %Y')
                    }
    # =========================================================

    # --- ALGORITHME D'AUTO-SCALING ---
    def get_auto_limits(tendance_series, ref_series, force_zero=False, upper_min=None):
        q_min = tendance_series.quantile(0.005)
        q_max = tendance_series.quantile(0.995)
        
        if isinstance(ref_series, pd.Series):
            ref_min = ref_series.min()
            ref_max = ref_series.max()
        else:
            ref_min = ref_series
            ref_max = ref_series
            
        effective_min = min(q_min, ref_min)
        effective_max = max(q_max, ref_max)
        
        margin = (effective_max - effective_min) * 0.15 
        if margin == 0: margin = effective_max * 0.1
        
        y_bottom = 0 if force_zero else effective_min - margin
        y_top = effective_max + margin
        if upper_min: y_top = max(y_top, upper_min)
        
        y_bottom = max(0, y_bottom)
        return [y_bottom, y_top]

    # --- PANORAMA GRAPHIQUE ---
    fig, axes = plt.subplots(6, 1, figsize=(16, 36), sharex=True)
    
    indicateurs = [
        ('v_peak', '1. Signature de l\'Impact (Vibration)', 'v_peak_ref', 'tab:purple', 
         get_auto_limits(df_lignes['tendance_v_peak'], df_lignes['v_peak_ref']), 
         "Ce que c'est : Le pic vibratoire maximal (\"le choc\") encaissé par la machine.\nMéthode : Accélération maximale filtrée | Lissage : Moyenne Mobile Exponentielle (EMA)"),
        
        ('i_nervosite', '2. Volatilité du Signal (Nervosité)', 'i_nervosite_ref', 'tab:orange', 
         get_auto_limits(df_lignes['tendance_i_nervosite'], df_lignes['i_nervosite_ref']), 
         "Ce que c'est : L'instabilité du courant électrique dans le moteur pendant la phase forte.\nMéthode : Moyenne des variations absolues de l'intensité, rééchantillonnée à 500ms."),
        
        ('t_elec_ms', '3. Time-to-Peak (Réactivité Électrique)', 't_elec_ms_ref', 'tab:green', 
         get_auto_limits(df_lignes['tendance_t_elec_ms'], df_lignes['t_elec_ms_ref']), 
         "Ce que c'est : Le temps que met l'intensité électrique pour atteindre son pic.\nMéthode : Chronométrage de la montée en couple depuis t0 jusqu'au pic d'intensité."),
        
        ('t_latence_ms', '4. Temps de Réaction (Latence Hydraulique)', 't_latence_ms_ref', 'tab:cyan', 
         get_auto_limits(df_lignes['tendance_t_latence_ms'], df_lignes['t_latence_ms_ref'], force_zero=True), 
         "Ce que c'est : Le délai exact entre la phase électrique intense et le coup de bélier vibratoire.\nMéthode : Décalage commande/réponse | Enveloppe sur 15s pour supprimer les accroches sur bruit parasite."),
        
        ('t_relax_ms', '5. Queue de Comète (Stabilisation)', 't_relax_ms_ref', 'tab:olive', 
         get_auto_limits(df_lignes['tendance_t_relax_ms'], df_lignes['t_relax_ms_ref']), 
         "Ce que c'est : Le temps d'amortissement nécessaire pour que la machine revienne au calme après la secousse.\nMéthode : Durée entre le pic vibratoire et la fin de l'événement | Corrélé à la santé des plots anti-vibratoires."),
        
        ('ipt_global', '6. Synthèse : Indice de Performance de Transit (IPT par Z-Score)', 1.0, 'navy', 
         get_auto_limits(df_lignes['tendance_ipt_global'], 1.0, upper_min=params['seuil_critique'] + 0.1), 
         "Ce que c'est : La note de santé globale de la machine (Z-Score pondéré des 3 premiers indicateurs ci-dessus).\nDéfinition : S'étalonne à neuf sur chaque époque post-maintenance. L'IPT englobe physiquement toutes les dérives.")
    ]

    for i, (col, titre, ref_col, couleur, y_limits, desc) in enumerate(indicateurs):
        ax = axes[i]
        tendance = df_lignes[f'tendance_{col}']
            
        if isinstance(ref_col, str):
            ax.plot(df_lignes['timestamp'], df_lignes[ref_col], color='black', linestyle='--', lw=1.5, alpha=0.5, label='Nominal Auto-Local (15j)')
        else:
            ax.axhline(y=ref_col, color='black', linestyle='-', lw=1.5, alpha=0.5, label='Seuil de base (1.0)')

        ax.plot(df_lignes['timestamp'], tendance, color=couleur, 
                linestyle='None', marker='o', markersize=3, alpha=0.7, 
                label='Points de Tendance (EMA)')
        
        if 'ipt' in col:
            ax.axhline(y=params['seuil_alerte'], color='orange', linestyle='--', alpha=0.7, label=f'Alerte ({params["seuil_alerte"]})')
            ax.axhline(y=params['seuil_critique'], color='red', linestyle='--', alpha=0.7, label=f'Critique ({params["seuil_critique"]})')

            # --- AFFICHAGE DU RUL ---
            if rul_info is not None:
                # Stopper l'extension à +15 jours maximum pour ne pas écraser les 6 graphiques historiques
                current_xlim = ax.get_xlim()
                max_x_display = current_xlim[1] + 15 
                
                # Coordonnées du dessin
                if rul_info['x_critique'] > max_x_display:
                    y_border = rul_info['a'] * max_x_display + rul_info['b']
                    x_plot = [rul_info['x_current'], max_x_display]
                    y_plot = [rul_info['y_current'], y_border]
                    
                    # On place la croix et le texte sur la bordure de sortie
                    mark_x, mark_y = max_x_display, y_border
                else:
                    x_plot = [rul_info['x_current'], rul_info['x_critique']]
                    y_plot = [rul_info['y_current'], rul_info['y_critique']]
                    mark_x, mark_y = rul_info['x_critique'], rul_info['y_critique']

                ax.plot(x_plot, y_plot, color='magenta', linestyle='-.', linewidth=2.5, label='Projection Linéaire (RUL)')
                ax.plot(mark_x, mark_y, marker='>', color='magenta', markersize=12) # Fleche de sortie
                
                # Bloquer l'échelle à max_x_display
                ax.set_xlim(current_xlim[0], max_x_display)
                
                # Texte RUL
                bbox_props = dict(boxstyle="round,pad=0.5", fc="black", ec="magenta", lw=2, alpha=0.8)
                msg = f"⚠ RUL ESTIMÉ : {int(rul_info['rul_days'])} Jours\nDate colmatage : {rul_info['date_critique']}"
                
                ax.annotate(msg, xy=(mark_x, mark_y),
                            xytext=(mark_x, params['seuil_critique'] + 0.04), textcoords='data',
                            ha='right', va='bottom',
                            bbox=bbox_props, color='white', weight='bold', fontsize=11,
                            arrowprops=dict(arrowstyle="->", connectionstyle="angle,angleA=0,angleB=90,rad=10", color="magenta", lw=2))
            else:
                bbox_props = dict(boxstyle="round,pad=0.5", fc="black", ec="green", lw=1.5, alpha=0.8)
                ax.annotate("✅ RUL : EN BONNE SANTÉ\n(Dégradation non mesurable)", 
                            xy=(0.02, 0.70), xycoords='axes fraction',
                            bbox=bbox_props, color='white', weight='bold', fontsize=11)
            # ------------------------

        ax.set_ylim(y_limits)
        tracer_maintenance_et_arrets(ax, params, arrets_prolonges)
        
        ax.set_title(titre, fontsize=16, fontweight='bold', loc='left', pad=15)
        
        # Positionner la légende à l'extérieur, mais alignée correctement
        ax.legend(loc='upper left', bbox_to_anchor=(1.0, 1.05), fontsize=10)
        
        ax.grid(True, linestyle=':', alpha=0.6)
        ax.grid(True, alpha=0.15, linestyle=':')

        # Dates sur TOUS les graphiques : forcer l'affichage des labels malgré sharex=True
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
        ax.tick_params(axis='x', labelbottom=True, rotation=45, labelsize=10)

        # Description placée via xlabel (s'insère NATURELLEMENT sous les ticks, pas de chevauchement)
        ax.set_xlabel(desc, fontsize=10, linespacing=1.4, labelpad=15,
                      bbox=dict(facecolor='whitesmoke', alpha=0.8, boxstyle='round,pad=0.6', edgecolor='lightgray'))

    fig.suptitle("Rapport de Santé Synthétique (Local MLOps V5)", fontsize=26, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.subplots_adjust(top=0.93, hspace=1.0, right=0.82)
    
    plt.savefig("panorama_sante_officiel_V5_Piecewise.png", dpi=300, bbox_inches='tight')
    logging.info(" === EXÉCUTION TERMINÉE : panorama_sante_officiel_V5_Piecewise.png === ")
    
if __name__ == "__main__":
    calculer_ipt_global()