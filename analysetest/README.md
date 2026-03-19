# Projet Analyste - Pipeline ETL

Bienvenue dans le projet d'analyse financière et boursière.

## Architecture

Le projet est divisé en 4 couches :
1. **Extraction (Data Ingestion)** : Scraping et appels API (`/scrapers`)
2. **Stockage (Data Warehouse)** : Base de données SQL (`/db`)
3. **Moteur de Calcul (Scoring)** : Transformation Pandas et calcul de score (`/engine`)
4. **Visualisation** : Intégration Power BI (Vues ou exports CSV)
