# 📊 État de l'environnement Python (PC PERSONNEL)

Voici l'inventaire des composants installés sur ce PC pour servir de point de comparaison avec votre configuration professionnelle.

## 🐍 Version de Python
- **Python :** 3.14.3 (Windows)

## 📦 Bibliothèques Principales
| Bibliothèque | Version Actuelle | Rôle |
| :--- | :--- | :--- |
| **pandas** | 3.0.2 | Manipulation de données (Gestion du `rolling`) |
| **numpy** | 2.4.4 | Calcul numérique (Trapezoïde, etc.) |
| **sqlalchemy** | 2.0.49 | Connexion database |
| **scipy** | 1.17.1 | Calculs de signaux |
| **scikit-learn** | 1.8.0 | Algorithmes statistiques (Z-Scores) |
| **matplotlib** | 3.10.8 | Génération des graphiques (Panorama) |

---

## 🛠️ Guide de mise à jour (À faire sur votre PC PRO)

Pour aligner votre PC professionnel sur cet environnement et garantir le bon fonctionnement des graphiques, ouvrez un terminal (PowerShell ou CMD) sur votre PC PRO et exécutez ces commandes :

### 1. Mise à jour de Python (Si nécessaire)
Si vous êtes sur une version de Python < 3.10, certaines optimisations de Pandas peuvent être bridées.
> [!NOTE]
> Il est recommandé d'utiliser au moins Python 3.11+ pour les meilleures performances sur de gros fichiers (4.3 Go).

### 2. Forcer l'alignement des bibliothèques
Copiez-collez cette commande unique pour tout mettre à jour d'un coup :

```powershell
pip install --upgrade pandas numpy sqlalchemy scipy scikit-learn matplotlib
```

### 3. Vérifier les versions installées
Une fois la commande terminée, vérifiez que vous avez bien le nécessaire avec :

```powershell
pip show pandas numpy
```

> [!CAUTION]
> Si votre PC Professionnel utilise un environnement virtuel (`.venv`), assurez-vous de l'activer avant de lancer les commandes `pip`.
