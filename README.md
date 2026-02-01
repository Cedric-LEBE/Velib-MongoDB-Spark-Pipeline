# Vélib' MongoDB Realtime + Spark — TP NoSQL/PySpark

Ce projet met en place une pipeline pour l'ingestion de données en temps réel avec MongoDB et leur traitement avec Spark.
- **Docker** : MongoDB + ingestion temps réel (toutes les 1–5 minutes)
- **Local** : Jupyter + **Spark** pour KPI + dataset ML (lecture/écriture via `pymongo`)

## 1) Pré-requis
- Docker Desktop
- Python 3.10+ (venv/conda)

## 2) Démarrage (Mongo + ingestion temps réel)
```bash
docker compose up -d --build
docker compose ps
```

Mongo est exposé sur `localhost:27017`.

## 3) Vérifier que Mongo se remplit
```bash
docker exec -it velib-mongo mongosh
use velib
show collections
db.availability_raw.countDocuments()
db.stations.countDocuments()
```

## 4) Lancer Jupyter + Spark 
Crée un venv/conda puis installe les dépendances :
```bash
python3 -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"
```

Lance Jupyter :
```bash
jupyter lab
```

Ouvre :
- `notebooks/00_check_env.ipynb`
- `notebooks/01_spark_kpi_ml.ipynb`

## 5) Résultats produits
- KPI par station : collection Mongo `velib.kpi_station`
- Dataset ML : collection Mongo `velib.ml_dataset`

## 6) Arrêter et nettoyer
Pour arrêter les conteneurs Docker :
```bash
make down
```
