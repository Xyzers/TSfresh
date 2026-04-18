import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path("maintenance_pilote_400j.db")


def fetch_one(conn, query):
    cur = conn.execute(query)
    return cur.fetchone()


def table_exists(conn, table_name):
    row = fetch_one(
        conn,
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    )
    return row is not None


def safe_count(conn, table_name):
    return fetch_one(conn, f"SELECT COUNT(*) FROM {table_name}")[0]


def safe_min_max(conn, table_name, date_col):
    return fetch_one(conn, f"SELECT MIN({date_col}), MAX({date_col}) FROM {table_name}")


def print_header(title):
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def ratio_to_percent(numerator, denominator):
    if denominator <= 0:
        return 0.0
    return (numerator / denominator) * 100.0


def main():
    print_header("Diagnostic rapide pipeline maintenance predictive")
    print(f"Date analyse: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Base cible: {DB_PATH}")

    if not DB_PATH.exists():
        print("[ERREUR] Base SQLite introuvable. Lancez d'abord Update_Historian.py.")
        return

    conn = sqlite3.connect(DB_PATH)
    try:
        required_tables = {
            "raw_historian": "DateTime",
            "signatures_sante": "timestamp",
            "ipt_historique": "timestamp",
        }

        missing = [t for t in required_tables if not table_exists(conn, t)]
        if missing:
            print("\n[INFO] Tables manquantes:")
            for t in missing:
                print(f" - {t}")
            print("\nAction: executez les scripts dans l'ordre Update -> Calcul -> Scoring.")

        print_header("1) Volumetrie et couverture temporelle")
        counts = {}
        for table_name, date_col in required_tables.items():
            if not table_exists(conn, table_name):
                print(f"{table_name}: ABSENTE")
                counts[table_name] = 0
                continue

            n_rows = safe_count(conn, table_name)
            t_min, t_max = safe_min_max(conn, table_name, date_col)
            counts[table_name] = n_rows
            print(f"{table_name}: {n_rows} lignes | min={t_min} | max={t_max}")

        if table_exists(conn, "raw_historian"):
            print_header("2) Qualite des signaux bruts (nulls)")
            null_row = fetch_one(
                conn,
                """
                SELECT
                  SUM(CASE WHEN intensite IS NULL THEN 1 ELSE 0 END),
                  SUM(CASE WHEN vibration IS NULL THEN 1 ELSE 0 END),
                  SUM(CASE WHEN debit_entree IS NULL THEN 1 ELSE 0 END),
                  SUM(CASE WHEN vitesse IS NULL THEN 1 ELSE 0 END)
                FROM raw_historian
                """,
            )
            n_raw = max(counts.get("raw_historian", 0), 1)
            labels = ["intensite", "vibration", "debit_entree", "vitesse"]
            for label, n_null in zip(labels, null_row):
                pct = ratio_to_percent(n_null, n_raw)
                print(f"{label}: {n_null} nulls ({pct:.2f}%)")

        print_header("3) Ratios de conversion pipeline")
        n_raw = counts.get("raw_historian", 0)
        n_sig = counts.get("signatures_sante", 0)
        n_ipt = counts.get("ipt_historique", 0)

        sig_vs_raw = ratio_to_percent(n_sig, n_raw)
        ipt_vs_sig = ratio_to_percent(n_ipt, n_sig)

        print(f"signatures_sante / raw_historian: {n_sig}/{n_raw} ({sig_vs_raw:.6f}%)")
        print(f"ipt_historique / signatures_sante: {n_ipt}/{n_sig} ({ipt_vs_sig:.2f}%)")

        print_header("4) Aide au diagnostic")
        if n_raw == 0:
            print("- Aucun brut: probleme de collecte Historian probable.")
        elif n_sig == 0:
            print("- Aucune signature: filtres detection possiblement trop stricts.")
            print("  Priorite de controle: intensity_peak, trigger_jump, min_speed, flow_cutoff, filtre NEP.")
        elif n_sig < 10:
            print("- Tres peu de signatures detectees: comparer avec le journal terrain debourbage.")
            print("  Si ecart eleve, relire les seuils + valider les signaux capteurs.")
        else:
            print("- Nombre de signatures non nul. Verifier la densite par mois dans le panorama.")

        if n_sig > 0 and n_ipt == 0:
            print("- IPT vide alors que signatures existent: verifier filtrage t_latence_ms >= 0.")

        print("\nDiagnostic termine.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
