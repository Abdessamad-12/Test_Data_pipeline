# Chicago Taxi Trips : Datalake Medallion (Airflow · PySpark · MinIO)

Pipeline de données de bout en bout pour une entreprise de mobilité urbaine : ingestion de trajets de taxi depuis l'API SODA, nettoyage avec PySpark, agrégats BI prêts à l'emploi, exposés en SQL (DuckDB) et en dashboard (Streamlit).

Tout démarre avec une seule commande : `docker compose up -d --build`.

---

## 1. Architecture

![Architecture du pipeline](<images/Architecture.drawio (2).png>)

| Couche | Chemin MinIO | Contenu |
|---|---|---|
| **Bronze** | `datalake/bronze/taxi_trips/year=/month=/part-*.csv` | CSV bruts, non modifiés, partitionnés par mois |
| **Silver** | `datalake/silver/taxi_trips/year=/month=/` | Parquet typé, nettoyé, dédupliqué, avec `trip_date` |
| **Gold** | `datalake/gold/<table>/` | Parquet des agrégats pour la BI |
| **Exposition** | DuckDB + Streamlit | SQL sur les tables gold et dashboard |

### Tables gold

| Table | Contenu |
|---|---|
| `daily_revenue` | Trajets, revenu total, pourboires et revenu moyen par jour |
| `daily_trip_duration` | Durée moyenne et médiane (minutes) et distance moyenne par jour |
| `top_pickup_zones` | Classement des zones de prise en charge, avec le nom de la zone |
| `top_routes` | Top 20 des itinéraires (zone de prise en charge → zone de dépose) |

---

## 2. Données

- **Source** : [Chicago Taxi Trips 2013-2023](https://data.cityofchicago.org/Transportation/Taxi-Trips-2013-2023-/wrvz-psew), via l'API SODA.
- **Période** : **2023-01 à 2023-03**  (filtre sur `trip_start_timestamp`), soit environ **1,45 million de lignes**.
- **Téléchargement** : fait par le pipeline lui-même (tâche `ingest_bronze`), mois par mois, en pages de 50 000 lignes triées sur `:id` (pagination stable).
- **Reproductibilité** : la période est définie dans `src/common/config.py` (`START_MONTH`, `END_MONTH`). Pour tester seulement le téléchargement :

```bash
python -m ingestion.api_soda                         # période de config.py
python -m ingestion.api_soda --start 2023-01 --end 2023-03
```

Un fichier écrit à la même clé est écrasé : relancer l'ingestion ne crée pas de doublons.

---

## 3. Lancement (pas à pas)

### Commandes

```bash
git clone https://github.com/Abdessamad-12/Test_Data_pipeline.git
cd Test_Data_pipeline
rm .env.example .env
docker compose up -d --build
```

Aucun fichier `.env` n'est nécessaire : les valeurs par défaut sont dans le compose. Pour les surcharger, copier `.env.example` en `.env`.

Le DAG `taxi_pipeline` se déclenche **tout seul au démarrage** (`schedule="@once"`). Le run complet dure environ **15 minutes**.

| Service | URL | Accès |
|---|---|---|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| MinIO (console) | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Dashboard | http://localhost:8501 | : |

### Suivi et vérification

```bash
docker compose ps                          # état des services
docker compose logs -f airflow-scheduler   # suivi du run
```

1. Dans Airflow, les trois tâches passent au vert : `ingest_bronze → bronze_to_silver → silver_to_gold`.
2. Dans MinIO, le bucket `datalake` contient `bronze/`, `silver/` et `gold/`.
3. Quand le DAG est terminé, le dashboard affiche **1 274 071 trajets** (rafraîchir la page : avant la fin du run, il indique que la couche gold est vide).

### Relancer / repartir de zéro

- `@once` ne rejoue pas à chaque redémarrage : l'état du run est conservé dans PostgreSQL et les données dans le volume MinIO.
- Pour relancer le pipeline : bouton **Trigger DAG** dans Airflow.
- Pour repartir de zéro : `docker compose down -v && docker compose up -d --build`.

### Interroger la couche SQL en ligne de commande

```bash
docker compose exec dashboard python -m serving.sql_engine \
  "SELECT trip_date, trips, total_revenue FROM daily_revenue ORDER BY total_revenue DESC LIMIT 5"
```

---

## 4. Structure du repo

```
.
├── docker-compose.yml        # MinIO, PostgreSQL, Airflow (init, webserver, scheduler), dashboard
├── Dockerfile                # image Airflow : Java 17 + PySpark 3.5 + jars S3A
├── requirements.txt          # dépendances de l'image Airflow
├── dags/
│   └── taxi_pipeline.py      # DAG : ingest_bronze → bronze_to_silver → silver_to_gold
└── src/
    ├── common/
    │   ├── config.py         # configuration unique (MinIO, bucket, période, chemins du lake)
    │   ├── storage.py        # client MinIO, création du bucket
    │   └── spark_session.py  # fabrique de SparkSession (config MinIO / S3A)
    ├── ingestion/
    │   └── api_soda.py       # API SODA → bronze
    ├── transformation/
    │   ├── bronze_to_silver.py
    │   └── silver_to_gold.py
    ├── reference/
    │   └── community_areas.csv   # 77 zones de Chicago (numéro → nom)
    └── serving/
        ├── sql_engine.py     # DuckDB : vues SQL sur les tables gold dans MinIO
        ├── dashboard.py      # application Streamlit
        ├── Dockerfile        # image du dashboard
        └── requirements.txt
```

Le DAG ne contient aucune logique métier : il appelle les fonctions `run()` des modules, testables séparément en local.

---

## 5. Traitements par couche

### Bronze : ingestion
Fichiers CSV écrits tels que reçus de l'API, sans modification, partitionnés par `year=/month=` à partir du filtre sur `trip_start_timestamp`.

### Silver : parsing, typage, nettoyage, déduplication
Lecture CSV **sans `inferSchema`** (tout en texte, puis `cast` explicite colonne par colonne) : pas de première lecture complète pour deviner les types, et un schéma stable.

| Étape | Traitement |
|---|---|
| Normalisation | `trim` des textes, chaînes vides → `NULL`, `tips`, `tolls`, `extras` nuls → `0` |
| Typage | timestamps, entiers (durée, zones), décimaux (distance, montants) |
| Qualité | clé et dates présentes, fin ≥ début, **durée de 60 s à 4 h**, **distance de 0,1 à 100 miles**, montants positifs ou nuls, **total ≤ 1 000 $** |
| Déduplication | `dropDuplicates(["trip_id"])` |
| Écriture | Parquet partitionné `year/month`, écrasement dynamique des partitions |

Bilan sur le trimestre : **1 446 439** lignes brutes → **1 274 071** lignes silver (environ 12 % écartées par les règles qualité, 0 doublon). 

### Gold : jointures et agrégations métier
- La durée moyenne est calculée sur `trip_seconds` et non sur `fin - début` : **le dataset arrondit les horodatages à 15 minutes** (de nombreux trajets ont `début = fin`).
- La table de zones (77 lignes) est jointe avec un **`broadcast join`** : aucun shuffle de la silver.

- Dans `top_routes`, l'agrégation précède la jointure, qui se fait donc sur quelques milliers de couples de zones et non sur 1,3 million de trajets.

- Contrôle de cohérence : la somme des trajets de `daily_revenue` est égale au nombre de lignes de la silver (écart 0).

### Exposition : SQL et dashboard
DuckDB lit les Parquet gold **directement dans MinIO**, sans Spark, et en fait des vues SQL (`daily_revenue`, `daily_trip_duration`, `top_pickup_zones`, `top_routes`). Streamlit s'appuie sur cette connexion et propose des graphiques (revenus, durées, zones et routes) et un onglet **SQL libre** limité aux requêtes `SELECT`.

Exemple :

```sql
SELECT date_trunc('month', trip_date) AS month, SUM(trips) AS trips, ROUND(SUM(total_revenue), 0) AS revenue
FROM daily_revenue GROUP BY 1 ORDER BY 1;
```

![SQL libre](<images/sql libre.png>)

---

## 6. Choix techniques

| Stack | Raison |
|---|---|
| **Airflow**  | Orchestrateur standard, DAG lisible, relance par tâche |
| **PySpark** | Démarrage rapide, aucun cluster à maintenir. Le code ne dépend pas du mode : passer à un cluster ne change que le `master` de la session |
| **MinIO** comme stockage objet | Datalake local compatible avec le connecteur Hadoop S3A (`s3a://`) |
| **Parquet** pour silver et gold | Colonnaire, compressé, typé |
| **DuckDB + Streamlit** pour l'exposition | Couche SQL légère qui lit les Parquet sans Spark, une image séparée de quelques centaines de Mo |

---

## 7. Ce que je ferais avec plus de temps

- **Qualité de données** : tests déclaratifs avec Great Expectations ou autre, et table des lignes rejetées avec le motif (`reject_reason`) pour auditer les écarts.
- **Format de table transactionnel** (Delta Lake ou Iceberg) : `MERGE` pour des mises à jour incrémentales, *time travel*, évolution de schéma.
- **Ingestion incrémentale** : un run mensuel planifié (`@monthly`, avec `catchup`) au lieu d'une tranche fixe.
- **Exploitation** : alertes sur échec de tâche, contrôles de fraîcheur, gestion des secrets (pas d'identifiants par défaut), image Airflow épinglée par digest.
- **Tests automatisés et CI** : tests unitaires (`month_bounds`, règles qualité sur un petit DataFrame), lint, build de l'image et exécution du pipeline sur un échantillon dans GitHub Actions.
- **Qualité du référentiel** : vérifier le fichier des 77 zones contre la source officielle de la Ville de Chicago.
- **Cluster Spark** (standalone, Kubernetes ou Databricks) pour séparer calcul et orchestration.

---------
## 8. Contraintes de ressources

### Problème rencontré

Spark tourne en mode local : le driver et les tâches d'exécution partagent la mémoire de la machine. Avec la configuration par défaut (`local[*]`, 200 partitions de shuffle, mémoire non plafonnée), le traitement de 1,45 million de lignes saturait les ressources de WSL.

### La solution que j'ai fait

Les réglages sont centralisés dans `src/common/spark_session.py`, donc appliqués à tous les jobs :

- 2 tâches en parallèle au lieu de tous les cœurs : moins de mémoire et de CPU consommés en même temps.
- Protège contre un `collect()` trop volumineux.
- Remplace les 200 partitions par défaut, adaptées à un cluster et non à une seule machine.
- Lecture par petits blocs : chaque tâche manipule moins de données à la fois.
- Limite le nombre de connexions simultanées vers MinIO.

**!!** : Cela rend le job un peu **plus lent**, mais le pipeline termine de façon fiable sur une machine de 8 Go (durée mesurée du run complet : **15 minutes**). 

**Machine avec plus de 8 Go de RAM** : augmenter le parallélisme (`local[4]`, la mémoire du driver (`spark.driver.memory` ) et le nombre de partitions de shuffle (`spark.sql.shuffle.partitions`) réduit le temps d'exécution.

