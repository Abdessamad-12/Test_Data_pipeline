# Chicago Taxi Trips : Datalake Medallion (Airflow · PySpark · MinIO)

Pipeline de données de bout en bout pour une entreprise de mobilité urbaine : ingestion de trajets de taxi depuis l'API SODA, nettoyage avec PySpark, agrégats BI prêts à l'emploi, exposés en SQL (DuckDB) et en dashboard (Streamlit).

Tout démarre avec une seule commande : `docker compose up -d --build`.

---

## 1. Architecture

![Architecture du pipeline](<Architecture.drawio (2).png>)

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
- **Téléchargement** : fait par le pipeline lui-même (tâche `ingest_bronze`), mois par mois, en pages de 50 000 lignes triées sur `:id` (pagination stable), avec reprise automatique sur erreur réseau ou limite de débit.
- **Reproductibilité** : la période est définie dans `src/common/config.py` (`START_MONTH`, `END_MONTH`). Le même téléchargement peut être lancé à la main :

```bash
python -m ingestion.api_soda                         # période de config.py
python -m ingestion.api_soda --start 2023-01 --end 2023-03
```

Un fichier écrit à la même clé est écrasé : relancer l'ingestion ne crée pas de doublons.
Un token d'application SODA est facultatif (`SODA_APP_TOKEN` dans `.env`) : il relève le quota de l'API.

---

## 3. Lancement (pas à pas)

### Prérequis

- Docker et Docker Compose.
- **8 Go de RAM alloués à Docker** (Spark tourne dans le conteneur Airflow). Sous WSL : `memory=8GB` dans `C:\Users\<nom>\.wslconfig`, puis `wsl --shutdown`.
- Accès internet (API SODA, téléchargement des images et des jars au build).

### Commandes

```bash
git clone <url-du-repo>
cd <repo>
docker compose up -d --build
```

Aucun fichier `.env` n'est nécessaire : les valeurs par défaut sont dans le compose. Pour les surcharger, copier `.env.example` en `.env`.

Le DAG `taxi_pipeline` se déclenche **tout seul au démarrage** (`schedule="@once"`). Le run complet dure environ **18 minutes**.

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

Bilan sur le trimestre : **1 446 439** lignes brutes → **1 274 071** lignes silver (environ 12 % écartées par les règles qualité, 0 doublon). Les seuils sont choisis à partir de la distribution observée (durée max de 24 h, distance max de 899 miles, montant max de 9 124 $ avant filtrage) et modifiables en une ligne dans les constantes de `bronze_to_silver.py`.

### Gold : jointures et agrégations métier
- La durée moyenne est calculée sur `trip_seconds` et non sur `fin - début` : **le dataset arrondit les horodatages à 15 minutes** (de nombreux trajets ont `début = fin`).
- La table de zones (77 lignes) est jointe avec un **`broadcast join`** : aucun shuffle de la silver. Dans `top_routes`, l'agrégation précède la jointure, qui se fait donc sur quelques milliers de couples de zones et non sur 1,3 million de trajets.
- Les trajets sans zone de prise en charge (≈ 72 000, zones masquées par la ville de Chicago pour la confidentialité) sont conservés dans les revenus et les durées, et exclus du classement des zones. Leur nombre est écrit dans les logs.
- Contrôle de cohérence : la somme des trajets de `daily_revenue` est égale au nombre de lignes de la silver (écart 0).

### Exposition : SQL et dashboard
DuckDB lit les Parquet gold **directement dans MinIO**, sans Spark, et en fait des vues SQL (`daily_revenue`, `daily_trip_duration`, `top_pickup_zones`, `top_routes`). Streamlit s'appuie sur cette connexion et propose des graphiques (revenus, durées, zones et routes) et un onglet **SQL libre** limité aux requêtes `SELECT`.

Exemple :

```sql
SELECT date_trunc('month', trip_date) AS month, SUM(trips) AS trips, ROUND(SUM(total_revenue), 0) AS revenue
FROM daily_revenue GROUP BY 1 ORDER BY 1;
```

---

## 6. Choix techniques et arbitrages

| Choix | Raison | Arbitrage |
|---|---|---|
| **Airflow** (LocalExecutor + PostgreSQL) | Orchestrateur standard, DAG lisible, relance par tâche | Pas de Celery ni de Kubernetes : inutile pour un pipeline unique |
| **PySpark en `local[*]`** dans le conteneur Airflow | Un seul Dockerfile, démarrage rapide, aucun cluster à maintenir. Le code ne dépend pas du mode : passer à un cluster ne change que le `master` de la session | Airflow et Spark partagent la mémoire : `spark.driver.memory` est limité à 3 Go |
| **MinIO** comme stockage objet | Datalake local compatible avec le connecteur Hadoop S3A (`s3a://`) | Un seul nœud, sans réplication |
| **Parquet** pour silver et gold | Colonnaire, compressé, typé | Pas de format transactionnel (Delta / Iceberg) |
| **Schéma explicite, sans `inferSchema`** | Évite une lecture supplémentaire du CSV et garantit des types stables | Liste de colonnes à maintenir |
| **`partitionOverwriteMode=dynamic`** | Relancer un mois ne réécrit que ce mois : pipeline idempotent | : |
| **Une seule table silver, qualité par filtrage** | Silver propre et simple. Le nombre de lignes écartées est tracé dans les logs | Les lignes rejetées ne sont pas conservées (voir améliorations) |
| **Jars S3A dans l'image** | Pas de téléchargement `spark.jars.packages` à chaque session : build reproductible | Image plus lourde |
| **`@once` + période dans `config.py`** | `docker compose up` suffit, sans clic dans l'interface | Changer de période demande de modifier `config.py` |
| **DuckDB + Streamlit** pour l'exposition | Couche SQL légère qui lit les Parquet sans Spark, une image séparée de quelques centaines de Mo | Pas de serveur SQL multi-utilisateurs (Trino) ni d'outil BI complet (Superset) |
| **Configuration par variables d'environnement** | Même code en local et dans Docker : seul `MINIO_ENDPOINT` diffère (`localhost` ou `minio`) | : |

---

## 7. Exécution en local (hors Docker)

Utile pour développer et déboguer un job Spark seul.

```bash
python -m venv .venv && source .venv/bin/activate
sudo apt install -y openjdk-17-jre-headless        # Java 17 pour Spark
pip install -r requirements.txt duckdb streamlit plotly pandas

# jars du connecteur S3A (identique au Dockerfile)
JARS=$(python -c 'import pyspark,os;print(os.path.join(os.path.dirname(pyspark.__file__),"jars"))')
curl -fsSL -o $JARS/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
curl -fsSL -o $JARS/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

cp .env.example .env
docker compose up -d minio                         # seul MinIO est nécessaire
export PYTHONPATH=$PWD/src

python -m ingestion.api_soda
python -m transformation.bronze_to_silver
python -m transformation.silver_to_gold
streamlit run src/serving/dashboard.py
```

---

## 8. Ce que je ferais avec plus de temps

- **Qualité de données** : tests déclaratifs avec Great Expectations, et table des lignes rejetées avec le motif (`reject_reason`) pour auditer les écarts.
- **Format de table transactionnel** (Delta Lake ou Iceberg) : `MERGE` pour des mises à jour incrémentales, *time travel*, évolution de schéma.
- **Ingestion incrémentale** : un run mensuel planifié (`@monthly`, avec `catchup`) au lieu d'une tranche fixe.
- **Cluster Spark** (standalone, Kubernetes ou Databricks) pour séparer calcul et orchestration.
- **Tests automatisés et CI** : tests unitaires (`month_bounds`, règles qualité sur un petit DataFrame), lint, build de l'image et exécution du pipeline sur un échantillon dans GitHub Actions.
- **Exposition** : Trino ou Superset pour un accès SQL et BI multi-utilisateurs, avec une API REST (FastAPI) sur les agrégats.
- **Exploitation** : alertes sur échec de tâche, contrôles de fraîcheur, gestion des secrets (pas d'identifiants par défaut), image Airflow épinglée par digest.
- **Qualité du référentiel** : vérifier le fichier des 77 zones contre la source officielle de la Ville de Chicago.

---

## 9. Dépannage

| Symptôme | Cause probable | Action |
|---|---|---|
| `Connection refused` vers `localhost:9000` dans une tâche | `MINIO_ENDPOINT` mal défini dans le conteneur | `docker compose exec airflow-scheduler env \| grep MINIO_ENDPOINT` doit afficher `http://minio:9000` |
| `OutOfMemoryError` ou conteneur tué | Moins de 8 Go alloués à Docker | Augmenter la mémoire (`.wslconfig` sous WSL) |
| Dashboard : « couche gold vide » | Le DAG n'est pas terminé | Attendre la fin du run, puis rafraîchir |
| Erreurs 429 de l'API SODA | Limite de débit sans token | Ajouter `SODA_APP_TOKEN` dans `.env` |
| `ModuleNotFoundError` dans une tâche | Image construite avec d'anciennes dépendances | `docker compose build --no-cache` |
