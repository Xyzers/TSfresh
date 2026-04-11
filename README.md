# 🚀 Projet TSFresh Antigravity : Maintenance Prédictive

Ce projet est un outil d'analyse de données industrielles dédié à la **maintenance prédictive de centrifugeuses**. Il permet de détecter automatiquement les cycles de fonctionnement (débourbages, lavages NEP) à partir des signatures électriques et vibratoires, d'extraire des indicateurs de santé et de calculer un score de performance (**IPT - Indice de Performance Technique**).

## 🎯 Objectifs
- **Surveillance automatisée** : Récupération des données haute fréquence depuis Wonderware Historian.
- **Analyse algorithmique** : Détection des événements réels et élimination du bruit de fond.
- **Indice de Santé** : Calcul de la dérive par rapport à un état de référence ("Golden Batch").
- **Anticipation** : Alerte prématurée avant l'apparition de pannes mécaniques majeures.

## 📂 Structure du Projet 

| Fichier | Description |
| :--- | :--- |
| **`Update_Historian.py`** | **Connecteur de données**. Télécharge les points des capteurs (intensité, vibration, vitesse) depuis le SQL Server industriel vers une base locale SQLite. |
| **`Calcul_des_signatures.py`** | **Moteur de détection**. Analyse le courant pour identifier les cycles de travail, calcule les métriques (nervosité, latence) et nettoie les données. |
| **`Scoring_Global_IPT.py`** | **Analyste statistique**. Calcule le score IPT global et génère les visualisations de l'état de santé du système. |
| **`Config.ini`** | **Configuration**. Contient les paramètres de connexion, les noms des Tags industriels et les seuils de détection. |
| **`Rapport_Versions.md`** | **Journal de bord**. Documentation technique sur les versions des librairies et les mises à jour de l'environnement. |
| **`requirements.txt`** | **Dépendances**. Liste les bibliothèques Python nécessaires (`tsfresh`, `pandas`, `sqlalchemy`, etc.). |

## 🛠️ Installation

### 1. Prérequis
- **Python 3.11+**
- Accès réseau au serveur Historian (pour la synchronisation).

### 2. Mise en place
Ouvrez un terminal dans le dossier du projet et installez les dépendances :
```powershell
pip install -r requirements.txt
```

### 3. Configuration
1. Copiez le fichier d'exemple :
   ```powershell
   cp Config.example.ini Config.ini
   ```
2. Éditez le fichier `Config.ini` pour renseigner vos paramètres spécifiques (accès base de données, noms des capteurs). **Note :** Le fichier `Config.ini` est ignoré par Git pour protéger vos identifiants.

## 🚀 Utilisation (Ordre d'exécution)

Pour effectuer une analyse complète, les scripts doivent être lancés dans l'ordre suivant :

1. **Récupération des données** :
   ```powershell
   python Update_Historian.py
   ```
2. **Traitement et détection des signatures** :
   ```powershell
   python Calcul_des_signatures.py
   ```
3. **Calcul du score IPT et visualisation** :
   ```powershell
   python Scoring_Global_IPT.py
   ```

## 📊 Indicateurs Clés (Métriques)
Le système surveille particulièrement :
- **v_peak** : Pics vibratoires durant les événements.
- **i_nervosite** : Instabilité du courant (indicateur d'usure mécanique).
- **IPT** : Score global (Alerte si > 1.3, Critique si > 1.6).
