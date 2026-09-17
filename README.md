# Realtime Crypto Streaming Pipeline

Pipeline de données de bout en bout combinant streaming temps réel et traitement batch ETL, entièrement conteneurisé avec Docker.

## Architecture
Binance API (/api/v3/trades)
|
Producer Python (polling + dedup)
|
Kafka (topic: crypto_transactions)
|
Consumer Python (transformation)
|
Cassandra (2 tables dénormalisées)
|
ETL Python (extraction périodique, toutes les 5 min)
|
PostgreSQL (bougies OHLC)

## Stack technique

- **Source de données** : API publique Binance (`/api/v3/trades`), transactions crypto réelles, sans clé API
- **Kafka + Zookeeper** : bufferisation et distribution des messages
- **Cassandra** : stockage NoSQL orienté colonnes pour les transactions brutes, haut volume d'écriture
- **PostgreSQL** : stockage analytique pour les données agrégées (bougies OHLC)
- **Python** : producer, consumer, ETL (`kafka-python`, `scylla-driver`, `psycopg2-binary`, `requests`, `tenacity`)
- **Docker Compose** : orchestration de l'ensemble des services

## Modélisation des données

### Cassandra (keyspace `crypto_streaming`)

Modélisation query-first : les tables sont conçues autour des requêtes à exécuter, pas des relations entre entités.

- `transactions_by_symbol` (clé de partition `symbol`, clustering `trade_time DESC`) : lister les transactions d'une paire, triées par date
- `transactions_by_id` (clé de partition `trade_id`) : récupérer une transaction précise par son ID

Les deux tables contiennent les mêmes données (dénormalisation volontaire), chacune optimisée pour sa requête.

### PostgreSQL (base `crypto_analytics`)

- `ohlc_candles` : bougies OHLC (Open/High/Low/Close) par intervalle de 5 minutes, calculées à partir des transactions brutes de Cassandra. Chargement idempotent via `UPSERT` (`ON CONFLICT ... DO UPDATE`).

## Composants

- **Producer** : interroge l'API Binance à intervalle régulier, déduplique les transactions côté client (l'endpoint `/trades` ne supporte pas de pagination par curseur), publie chaque nouvelle transaction dans Kafka.
- **Consumer** : lit le topic Kafka en continu, transforme les données (conversion en `Decimal` pour les montants, `datetime` UTC pour les timestamps), écrit dans les deux tables Cassandra.
- **ETL** : s'exécute en boucle toutes les 5 minutes, extrait les transactions récentes de Cassandra, les agrège en bougies OHLC, les charge dans PostgreSQL.

## Fiabilité

- **Logging structuré JSON** sur les trois services (producer, consumer, ETL), via un module de logging partagé (`common/logger.py`).
- **Retry avec backoff exponentiel** (librairie `tenacity`) sur les appels à l'API Binance et les connexions à Cassandra/PostgreSQL.
- **Redémarrage automatique** des conteneurs applicatifs en cas d'échec (`restart: on-failure`).

## Tests

Suite de tests unitaires (`pytest`) couvrant la logique de transformation :
- Conversion des types (prix en `Decimal`, timestamps en `datetime` UTC)
- Calcul des bougies OHLC (open/high/low/close/volume/trade_count)
- Arrondi des timestamps à leur intervalle de 5 minutes

## Lancer le projet

Prérequis : Docker et Docker Compose installés.

```bash
git clone https://github.com/boulakhbarnisrineeee/realtime-streaming-pipeline.git
cd realtime-streaming-pipeline
docker-compose up -d --build
```

### Initialiser les schémas (première utilisation)

```bash
docker cp cassandra/init.cql cassandra:/init.cql
docker exec -it cassandra cqlsh -f /init.cql

docker cp postgres/init.sql postgres:/init.sql
docker exec -it postgres psql -U etl_user -d crypto_analytics -f /init.sql
```

### Vérifier que ça fonctionne

```bash
docker logs producer --tail 20
docker logs consumer --tail 20
docker logs etl --tail 20
docker exec -it cassandra cqlsh -e "SELECT * FROM crypto_streaming.transactions_by_symbol LIMIT 5;"
docker exec -it postgres psql -U etl_user -d crypto_analytics -c "SELECT * FROM ohlc_candles;"
```

### Lancer les tests

```bash
pip install pytest tenacity psycopg2-binary python-json-logger scylla-driver kafka-python
python -m pytest tests/ -v
```