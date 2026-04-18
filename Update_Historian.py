import pandas as pd
import sqlalchemy
from sqlalchemy.engine import URL
from datetime import datetime
import logging
import configparser
import sqlite3

# Configuration du journal
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def ensure_vitesse_column(db_path):
    """Vérifie si la colonne 'vitesse' existe, sinon l'ajoute (Sécurité)"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(raw_historian)")
    columns = [column[1] for column in cursor.fetchall()]
    
    if 'vitesse' not in columns:
        logging.info(" Ajout de la colonne manquante 'vitesse' à la base locale...")
        try:
            conn.execute("ALTER TABLE raw_historian ADD COLUMN vitesse REAL")
            conn.commit()
            logging.info(" Colonne 'vitesse' ajoutée avec succès.")
        except Exception as e:
            logging.error(f" Erreur lors de la modification de la table : {e}")
    conn.close()

def get_last_timestamp(db_path):
    """Trouve la dernière date enregistrée pour ne télécharger que la nouveauté"""
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute("SELECT MAX(DateTime) FROM raw_historian")
        last_dt = cursor.fetchone()[0]
    except sqlite3.OperationalError:
        last_dt = None
    conn.close()
    return last_dt

def get_engine(config_path='Config.ini'):
    """Crée le moteur de connexion DYNAMIQUE à Wonderware Historian"""
    config = configparser.ConfigParser()
    config.read(config_path)
    
    # On lit les identifiants dans le Config.ini
    driver = config.get('DB', 'odbc_driver')
    server = config.get('DB', 'db_host')
    database = config.get('DB', 'db_name')
    uid = config.get('DB', 'db_user')
    pwd = config.get('DB', 'db_password')
    
    connection_string = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={uid};PWD={pwd}"
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    return sqlalchemy.create_engine(connection_url)

def update_local_database(db_path="maintenance_pilote_400j.db", config_path="Config.ini"):
    ensure_vitesse_column(db_path)
    last_dt = get_last_timestamp(db_path)
    
    config = configparser.ConfigParser()
    config.read(config_path)
    
    configured_start = pd.to_datetime(config.get('Period', 'start_time', fallback='2025-01-19 00:00:00'))
    end_cfg_raw = config.get('Period', 'end_time', fallback='').strip()
    configured_end = pd.to_datetime(end_cfg_raw) if end_cfg_raw else pd.Timestamp(datetime.now())

    if not last_dt:
        start_dt = configured_start
    else:
        # Evite de retraiter en dehors de la fenetre demandee
        start_dt = max(pd.to_datetime(last_dt), configured_start)

    now = min(pd.Timestamp(datetime.now()), configured_end)

    if start_dt >= now:
        logging.info(f" Aucune mise a jour necessaire (start={start_dt}, end={now}).")
        return
    
    logging.info(f" Debut de l'importation par lots de {start_dt} a {now}...")

    tags = {
        'intensite': config.get('Tags', 'intensite'),
        'vibration': config.get('Tags', 'vibration'),
        'debit_entree': config.get('Tags', 'debit_entree'),
        'vitesse': config.get('Tags', 'vitesse')
    }
    
    engine = get_engine(config_path)
    tag_list = "', '".join(tags.values())
    inv_tags = {v: k for k, v in tags.items()}
    
    # --- DÉCOUPAGE TEMPOREL (CHUNKING) : 15 JOURS PAR REQUÊTE ---
    chunk_size = pd.Timedelta(days=15)
    current_start = start_dt
    
    conn = sqlite3.connect(db_path)
    
    while current_start < now:
        current_end = min(current_start + chunk_size, now)
        logging.info(f" Téléchargement du bloc : {current_start.strftime('%Y-%m-%d')} au {current_end.strftime('%Y-%m-%d')}...")
        
        query = f"""
        SELECT DateTime, TagName, Value
        FROM History
        WHERE TagName IN ('{tag_list}')
          AND DateTime > '{current_start}' AND DateTime <= '{current_end}'
          AND wwRetrievalMode = 'Delta'
        """
        
        try:
            df_new = pd.read_sql(query, engine)
            if not df_new.empty:
                # Pivotement et Renommage
                df_pivot = df_new.pivot_table(index='DateTime', columns='TagName', values='Value').reset_index()
                df_pivot = df_pivot.rename(columns=inv_tags)
                
                # Sauvegarde immédiate du bloc (Libère la mémoire)
                df_pivot.to_sql('raw_historian', conn, if_exists='append', index=False)
                logging.info(f"    + {len(df_pivot)} lignes ajoutées.")
            else:
                logging.info("   ℹ Aucune donnée sur cette période.")
                
        except Exception as e:
            logging.error(f" Erreur sur ce bloc : {e}")
            break # On arrête proprement si la connexion lâche
            
        current_start = current_end # On passe au bloc suivant

    conn.close()
    logging.info(" Mise à jour de la base de données terminée !")

if __name__ == "__main__":
    update_local_database()
