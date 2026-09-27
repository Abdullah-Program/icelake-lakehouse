# IceLake Local Lakehouse Project - Checkpoint & Progress Log

## 📌 Project Overview
A production-grade, local Lakehouse architecture demonstrating:
- **Object Storage**: Garage S3 (lightweight, self-hosted, distributed S3-compatible storage)
- **Catalog**: Apache Polaris (Open-source Iceberg REST catalog)
- **Table Format**: Apache Iceberg 1.6.1 with hidden partitioning (`months(created_date)`)
- **Engines**: 
  - **Apache Spark 3.5** (PySpark running inside Docker `jupyter` container)
  - **Trino 440** (Distributed SQL query engine for BI/analytics)
- **Dataset**: Wikipedia articles (10,000 records across 12 monthly partitions from Jan 2024 to Dec 2024)

---

## 🛠️ Infrastructure & Credentials Reference

| Service | Host Port | Internal Port | Details / Web UI |
|---|---|---|---|
| **Garage S3 API** | `http://localhost:3900` | `3900` | S3 Endpoint (`http://garage:3900`) |
| **Garage Admin API**| `http://localhost:3903` | `3903` | Admin & Health status |
| **Polaris Catalog** | `http://localhost:8181` | `8181` | REST Catalog (`http://polaris:8181/api/catalog`) |
| **Trino Query Engine**| `http://localhost:8080` | `8080` | Trino Web UI (User: `admin` or `trino`) |
| **Jupyter / PySpark** | `http://localhost:8888` | `8888` | Token: `polaris` |

### S3 & Catalog Credentials
- **S3 Bucket**: `s3://icelake/`
- **AWS Access Key ID**: `GK1f5cbdb2ac0aebcbb74e18d5`
- **AWS Secret Access Key**: `b5057d94ab27275064a278e2e358bbe751d3610c26f7d6499bc5f0ca76996b29`
- **AWS Region**: `us-east-1`
- **Polaris Client ID**: `polaris`
- **Polaris Client Secret**: `polaris`
- **Polaris Warehouse**: `icelake`
- **Catalog Namespace**: `wikipedia`
- **Main Table**: `icelake.wikipedia.articles`

---

## 🚦 Phase Roadmap & Progress

- [x] **Phase 0: Architecture & Environment Setup**
  - Configured `docker-compose.yml` with Garage, Polaris, Trino, and Jupyter.
  - Initialized Garage S3 cluster layout, created bucket `icelake` and S3 API keys.
  - Set up Python virtual environment and `.env`.

- [x] **Phase 1: Ingestion & Raw Parquet Generation**
  - Downloaded 10,000 sample Wikipedia articles.
  - Partitioned raw data by `year_month=2024-01` through `2024-12` in `data/raw/`.

- [x] **Phase 2: Polaris REST Catalog Initialization**
  - Script: `src/catalog/polaris_setup.py`
  - Created internal catalog `icelake` pointing to `s3://icelake/` with `stsUnavailable: true` for Garage.
  - Created namespace `wikipedia`.

- [x] **Phase 3: Iceberg Table Creation & Initial Ingestion**
  - Script: `src/ingestion/create_iceberg_table.py`
  - Created Iceberg v2 table `icelake.wikipedia.articles` partitioned by `months(created_date)`.
  - Loaded all 10,000 records from `data/raw/` across 12 monthly partitions.
  - Committed initial snapshot `#6831925201516210010`.

- [x] **Phase 4: Multi-Engine Querying via Trino**
  - Configured `docker/trino/catalog/iceberg.properties` with Polaris REST catalog connection and S3 credentials.
  - Tested SQL queries across partitions using Trino.

- [x] **Phase 5: Lakehouse Table Maintenance (PySpark)**
  - Script: `src/maintenance/table_ops.py`
  - Ran compaction via `CALL icelake.system.rewrite_data_files()`.
  - Successfully expired old snapshots via `CALL icelake.system.expire_snapshots()`.
  - Verified snapshot history and partition state.

- [x] **Phase 6: Schema Evolution (PySpark & Trino)**
  - Script: `src/ingestion/schema_evolution.py`
  - Added column `category STRING` without rewriting existing Parquet data (defaults to `NULL`).
  - Renamed column `word_count` to `word_count_v2` using Iceberg field ID tracking.
  - Appended new records with new schema and verified unified reading of old and new files.
  - Verified Trino immediately discovers evolved schema via Polaris REST catalog.

- [x] **Phase 7: Benchmarking & Time Travel**
  - Script: `src/benchmark/run_benchmark.py`
  - **Partition Pruning**: Proved `months(created_date)` hidden partitioning speedup (3.03x faster in PySpark, scanning 1 partition instead of 12).
  - **Time Travel**: Verified querying `VERSION AS OF 8498576220958852406` (10,000 records) vs current snapshot `7273870742450103981` (10,002 records).
  - Output metrics saved to `data/benchmarks/results.csv` and `data/benchmarks/report.md`.

- [x] **Phase 8: Snowflake Multi-Engine Querying & Enterprise RBAC**
  - Script: `snowflake/setup.sql`
  - Executed in Snowflake Free Trial with warehouse `ICELAKE_WH`, database `ICELAKE_DB`, schema `WIKIPEDIA`.
  - Configured 3-tier enterprise RBAC hierarchy: `read_only_user` -> `data_analyst` -> `data_engineer` -> `SYSADMIN`.

---

## ⚡ Quick Start / Resume Commands

### 1. Ensure Docker Desktop is Running
Make sure Docker Desktop is launched on Windows.

### 2. Start Project Containers
```powershell
docker compose -f docker\docker-compose.yml start
# or if creating fresh:
docker compose -f docker\docker-compose.yml up -d
```

### 3. Run PySpark Scripts inside Docker
Because Spark needs connectivity to Garage and Polaris over the Docker network, scripts are executed inside the `jupyter` container:
```powershell
docker exec -it jupyter bash -c "cd /home/jovyan/work && python src/benchmark/run_benchmark.py"
```

### 4. Run Trino CLI Queries
```powershell
docker exec -it trino trino --catalog iceberg --schema wikipedia
```
Example query:
```sql
SELECT count(*), min(created_date), max(created_date) FROM articles;
```
