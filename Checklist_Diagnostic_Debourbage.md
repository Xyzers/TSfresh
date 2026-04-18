# Grille de Diagnostic Pas a Pas
## TSFresh Antigravity - Checklist Terrain + Check Script

Objectif: diagnostiquer rapidement un cas "tres peu de debourbages detectes" et separer:
- un probleme process/capteurs terrain,
- un probleme de collecte Historian,
- un probleme de filtrage dans `Calcul_des_signatures.py`.

Perimetre adapte au projet:
- Ingestion: `Update_Historian.py` -> table SQLite `raw_historian`
- Detection signatures: `Calcul_des_signatures.py` -> table `signatures_sante`
- Scoring panorama: `Scoring_Global_IPT.py` -> table `ipt_historique` + image `panorama_sante_officiel_V5_Piecewise.png`

---

## 1) Checklist Terrain (atelier / conduite)

### Etape T1 - Validation evenement reel
- [ ] Confirmer qu'un debourbage reel a bien ete lance (heure debut/fin)
- [ ] Confirmer que la machine etait en regime de production pendant l'evenement
- [ ] Noter si cycle NEP/lavage etait actif dans la meme fenetre (risque d'exclusion par filtre NEP)

Resultat attendu:
- Au moins 3 a 5 debourbages reels repertories avec horodatages fiables sur la periode analysee.

### Etape T2 - Coherence capteurs process
- [ ] Intensite: verifier qu'un pic net existe au lancement (seuil projet: `intensity_peak = 70A`)
- [ ] Vitesse: verifier que la machine depasse bien le minimum (`min_speed = 5600`)
- [ ] Debit entree: verifier phase de debit faible pendant l'evenement (`flow_cutoff = 1.5`)
- [ ] Vibration: verifier qu'un pic suit la phase electrique

Resultat attendu:
- Les 4 signaux montrent une sequence physique plausible sur chaque debourbage.

### Etape T3 - Verrouillage des causes terrain
- [ ] Absence de derive de calibration capteurs (intensite/vibration)
- [ ] Absence de microcoupures reseau automate/historian
- [ ] Pas de changement de recette/profil machine non reporte

Resultat attendu:
- Si terrain NOK -> corriger terrain avant tuning algorithmique.

---

## 2) Check Script (pipeline technique)

## Etape S1 - Preflight configuration
Fichier: `Config.ini`

- [ ] Tags exacts et actifs: `intensite`, `vibration`, `debit_entree`, `vitesse`
- [ ] Seuils detection coherents:
  - `trigger_jump = 12.0`
  - `hysteresis_margin = 10.0`
  - `intensity_peak = 70.0`
  - `min_duration_sec = 3`
  - `max_duration_sec = 480`
  - `min_speed = 5600.0`
  - `flow_cutoff = 1.5`
- [ ] Parametres NEP non trop agressifs:
  - `nep_burst_window_h = 3.0`
  - `nep_burst_min_events = 7`

Interpretation:
- Si peu de debourbages detectes, les 3 filtres les plus eliminants sont souvent `intensity_peak`, `min_speed`, `flow_cutoff`, puis le filtre NEP.

## Etape S2 - Verification ingestion Historian
Script: `Update_Historian.py`

- [ ] Lancement sans erreur
- [ ] Import par blocs 15 jours observe dans les logs
- [ ] Table `raw_historian` alimentee
- [ ] Colonne `vitesse` presente (ajoutee automatiquement si absente)

Commande:
```powershell
python Update_Historian.py
```

Controle SQLite rapide:
```sql
SELECT COUNT(*) AS n_raw FROM raw_historian;
SELECT MIN(DateTime) AS t_min, MAX(DateTime) AS t_max FROM raw_historian;
SELECT
  SUM(CASE WHEN intensite IS NULL THEN 1 ELSE 0 END) AS n_null_int,
  SUM(CASE WHEN vibration IS NULL THEN 1 ELSE 0 END) AS n_null_vib,
  SUM(CASE WHEN debit_entree IS NULL THEN 1 ELSE 0 END) AS n_null_debit,
  SUM(CASE WHEN vitesse IS NULL THEN 1 ELSE 0 END) AS n_null_vitesse
FROM raw_historian;
```

Seuil de decision:
- Si `n_raw` faible, ou plage temporelle trop courte -> probleme de collecte.

## Etape S3 - Verification detection signatures
Script: `Calcul_des_signatures.py`

- [ ] Lancement sans erreur
- [ ] Message de synthese "X signatures enregistrees"
- [ ] Pas de message final "Aucune signature n'a survecu aux filtres"

Commande:
```powershell
python Calcul_des_signatures.py
```

Controle SQLite cible:
```sql
SELECT COUNT(*) AS n_sig FROM signatures_sante;
SELECT MIN(timestamp) AS sig_min, MAX(timestamp) AS sig_max FROM signatures_sante;
SELECT
  AVG(intensite_max) AS avg_i_max,
  AVG(duree_sec) AS avg_duree,
  AVG(t_elec_ms) AS avg_t_elec,
  AVG(t_latence_ms) AS avg_t_lat,
  AVG(t_relax_ms) AS avg_t_relax
FROM signatures_sante;
```

Seuil de decision:
- Si `n_sig = 0`: filtrage trop strict ou signaux incoherents.
- Si `n_sig` tres faible vs realite terrain: verifier d'abord `intensity_peak`, `min_speed`, `flow_cutoff`, puis exclusion NEP.

## Etape S4 - Verification scoring et panorama
Script: `Scoring_Global_IPT.py`

- [ ] Lancement sans erreur
- [ ] Table `ipt_historique` bien creee
- [ ] Image `panorama_sante_officiel_V5_Piecewise.png` regeneree

Commande:
```powershell
python Scoring_Global_IPT.py
```

Controle SQLite:
```sql
SELECT COUNT(*) AS n_ipt FROM ipt_historique;
SELECT MIN(timestamp) AS ipt_min, MAX(timestamp) AS ipt_max FROM ipt_historique;
SELECT AVG(ipt_global) AS ipt_moy, MAX(ipt_global) AS ipt_max FROM ipt_historique;
```

Seuil de decision:
- Si `n_ipt` << `n_sig`, verifier exclusions et dates invalides (`t_latence_ms < 0` est retire).

---

## 3) Matrice Decisionnelle Rapide

### Cas A - `raw_historian` faible ou incomplet
Cause probable:
- Connectivite Historian / tags incorrects / plage temporelle non couverte.
Action:
- Corriger config DB/Tags, relancer `Update_Historian.py`.

### Cas B - `raw_historian` OK mais `signatures_sante` tres faible
Cause probable:
- Seuils detection trop severes ou filtre NEP trop agressif.
Action:
- Ajuster progressivement:
  1. `intensity_peak` (ex: 70 -> 65)
  2. `trigger_jump` (ex: 12 -> 10)
  3. `min_speed` (ex: 5600 -> 5400)
  4. `flow_cutoff` (ex: 1.5 -> 2.0)
  5. `nep_burst_min_events` (ex: 7 -> 9)

### Cas C - `signatures_sante` OK mais panorama parait pauvre
Cause probable:
- Distribution temporelle clairsemee, arrets longs, lissage EMA qui ecrase la variabilite.
Action:
- Verifier densite de points par mois, puis relire la periode analysee.

---

## 4) Check Script Automatise (optionnel, 5 minutes)

Creer un script de verification rapide qui affiche:
- volumetrie `raw_historian`, `signatures_sante`, `ipt_historique`
- couverture temporelle de chaque table
- pourcentage de valeurs nulles par signal brut
- ratio de conversion `signatures_sante / raw_historian` (ordre de grandeur)

Si vous le souhaitez, je peux generer ce script (`diagnostic_rapide.py`) dans le projet avec sortie console prete a l'emploi.

---

## 5) Ordre d'execution recommande en incident
1. `python Update_Historian.py`
2. Controle SQL de `raw_historian`
3. `python Calcul_des_signatures.py`
4. Controle SQL de `signatures_sante`
5. `python Scoring_Global_IPT.py`
6. Lecture du panorama final

Definition de resolution:
- La resolution est atteinte quand le nombre de signatures detectees retrouve la tendance terrain attendue, sans explosion de faux positifs NEP.
