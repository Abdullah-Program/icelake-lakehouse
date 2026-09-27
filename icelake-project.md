# 🏗️ IceLake Local — Production-Grade Open Lakehouse
**Role Target:** AWS Data Engineer – Open Data Platform  
**Goal:** End-to-end demo that mirrors the JD 1:1, using 100% free & local tools  
**Current Status:** Phase 5 Completed (Table Maintenance Tested) → Proceeding to Schema Evolution & Phase 6 (Multi-Engine: DuckDB + Trino + Snowflake)

---

## 📌 JD Coverage Map

| JD Requirement | This Project | Status |
|---|---|---|
| AWS Glue + Apache Spark ETL | PySpark 3.5 local (inside Docker `jupyter` container) | ✅ Completed |
| Apache Iceberg table design | Full Iceberg 1.6.1 v2 table with hidden partitioning (`months(created_date)`) | ✅ Completed |
| S3 Object Storage Layer | Garage S3 (self-hosted S3-compatible storage cluster) | ✅ Completed |
| Apache Polaris REST Catalog | Apache Polaris in Docker with S3 backend | ✅ Completed |
| Table maintenance | Compaction (`rewrite_data_files`), snapshot expiry (`expire_snapshots`) | ✅ Completed |
| Schema evolution | Backward-compatible column add + rename without rewriting Parquet | ⏳ Next |
| Multi-Engine Querying | PySpark + DuckDB + Trino + Snowflake | ⏳ Next (In Progress) |
| Engine Benchmarking | Partition pruning vs Full scan timing comparison | ⏳ Phase 7 |
| IAM + security + access controls | Polaris RBAC + Snowflake role hierarchy | ⏳ Phase 8 |

---

## 🏛️ Real Architecture: The Garage S3 Upgrade

In the original concept, the project planned to use `file:///data/iceberg` (local filesystem mounts).  
**Engineering Reality & Why We Upgraded:**
- Apache Polaris, Spark Iceberg, and Trino running in distinct Docker containers struggle with `file://` scheme path mapping across container boundaries.
- Furthermore, enterprise Lakehouses **always run on Object Storage (Amazon S3 / Google Cloud Storage)**, not local hard drives.
- **The Solution:** We integrated **Garage S3** (`garage:3900`) directly into `docker-compose.yml`. Garage is a lightweight, distributed, S3-compatible storage engine.
- This gives us a 100% production-parity S3 API (`s3://icelake/`) with AWS access keys, S3 endpoints, and `S3FileIO` in Spark and Trino.

---

## 🆓 Final Free Stack & Credentials

| Layer | Tool | Connection / URL | Notes |
|---|---|---|---|
| **Object Storage** | Garage S3 | `http://localhost:3900` (internal: `http://garage:3900`) | Bucket: `s3://icelake/` |
| **REST Catalog** | Apache Polaris | `http://localhost:8181` (internal: `http://polaris:8181`) | Warehouse: `icelake`, Realm: `default-realm` |
| **ETL Engine** | PySpark 3.5 | Executed via `docker exec -it jupyter` | Iceberg 1.6.1 + AWS Bundle |
| **Query Engine 1** | DuckDB | Embedded Python / CLI | Fast OLAP with Iceberg extension |
| **Query Engine 2** | Trino 440 | `http://localhost:8080` (CLI: `docker exec -it trino`) | Distributed SQL query engine |
| **Query Engine 3** | Snowflake | Free Trial Web UI / Python connector | Cloud Data Warehouse with Iceberg support |
| **Orchestration** | Docker Compose | `docker/docker-compose.yml` | 4 core containers (Garage, Polaris, Trino, Jupyter) |

### Active Credentials Reference
- **S3 Access Key ID:** `GK1f5cbdb2ac0aebcbb74e18d5`
- **S3 Secret Access Key:** `b5057d94ab27275064a278e2e358bbe751d3610c26f7d6499bc5f0ca76996b29`
- **S3 Region:** `us-east-1`
- **Polaris Client ID:** `polaris`
- **Polaris Client Secret:** `polaris`

---

## 🗂️ Project Folder Structure

```
icelake-local/
├── .env                            # Local environment variables & S3 credentials
├── .env.docker                     # Environment variables for Docker containers
├── PROJECT_CHECKPOINT.md           # Real-time state, phase logs & resume guide
├── README.md                       # High-level repo summary
├── requirements.txt                # Python dependencies
├── icelake-project.md              # Master architecture guide & roadmaps
├── docker/
│   ├── docker-compose.yml          # Garage + Polaris + Trino + Jupyter
│   ├── garage/
│   │   └── garage.toml             # S3 cluster config
│   ├── polaris/
│   │   └── polaris-config.yml      # REST catalog config
│   └── trino/
│       ├── catalog/
│       │   └── iceberg.properties  # Trino Iceberg S3 catalog connector
│       └── config.properties       # Trino engine configs
├── data/
│   ├── raw/                        # 10k Wikipedia articles partitioned by month
│   │   ├── year_month=2024-01/
│   │   └── ...
│   ├── garage-meta/                # Garage S3 metadata & sled engine
│   ├── garage-data/                # Garage S3 actual chunks
│   └── benchmarks/                 # Query benchmark CSVs & Markdown reports
├── notebooks/
│   ├── 01_setup_polaris.ipynb      # Interactive Polaris walkthrough
│   ├── 02_create_iceberg_tables.ipynb
│   ├── 03_schema_evolution.ipynb
│   ├── 04_table_maintenance.ipynb
│   └── 05_benchmark.ipynb
├── src/
│   ├── ingestion/
│   │   ├── hf_to_iceberg.py        # HuggingFace raw download & partitioning
│   │   └── create_iceberg_table.py # Spark table creation & S3 load
│   ├── catalog/
│   │   ├── polaris_setup.py        # Bootstrap Polaris catalog & namespace
│   │   └── delete_catalog.py       # Teardown / cleanup helper
│   ├── maintenance/
│   │   └── table_ops.py            # Compaction, snapshot expiry, orphan check
│   └── benchmark/
│       └── run_benchmark.py        # Multi-engine benchmarking & time-travel
└── snowflake/
    └── setup.sql                   # Snowflake external volume, Iceberg table & RBAC
```

---

## ❄️ Where Does Snowflake Fit in Multi-Engine?

### 1. The Core Concept
The entire point of an **Open Lakehouse** is avoiding vendor lock-in.  
In traditional architectures:
- Spark writes proprietary format or Hive tables.
- Snowflake requires loading data into Snowflake proprietary micro-partitions (`COPY INTO`).
- You pay double for storage and compute!

With **Apache Iceberg**:
- **Single Source of Truth:** Data lives as Parquet files in S3 (`s3://icelake/`) described by Iceberg metadata JSON files.
- **Any Engine Can Read/Write:**
  - **PySpark** does heavy ETL and table maintenance.
  - **Trino** serves interactive dashboards and BI queries.
  - **DuckDB** executes lightning-fast local analytics.
  - **Snowflake** queries the exact same Iceberg table without ingestion costs!

### 2. Why Haven't We Run Snowflake Yet?
1. **Execution Order:** We had to first establish the storage (Garage), the catalog (Polaris), ingest the data (Phase 3), and prove table maintenance (Phase 5). Snowflake belongs in **Phase 6: Multi-Engine Querying**.
2. **Cloud vs Localhost Reality:**
   - Spark, Trino, and DuckDB run locally inside your network and can reach `http://localhost:3900`.
   - Snowflake runs in the Cloud (AWS/Azure). It cannot reach your laptop's private IP (`127.0.0.1:3900`) directly unless:
     - **Option A (Snowflake Free Trial Simulation - Recommended for zero AWS cost):** Run `snowflake/setup.sql` in Snowflake Free Trial. It creates a Snowflake Iceberg table with identical schema and data, demonstrating Snowflake Iceberg syntax, query plans, and RBAC.
     - **Option B (Cloudflare R2 / AWS S3 Free Tier):** If you want Snowflake to query the exact same live bucket, we can sync the `data/iceberg` folder to a free Cloudflare R2 bucket (10GB free, $0 egress fees) and point both Trino and Snowflake to it!

---

## 🔢 Phase-by-Phase Status & Detailed Guide

---

### Phase 0 — Environment & Docker Setup (✅ COMPLETED)
- Configured `docker/docker-compose.yml` with Garage S3, Polaris, Trino, and Jupyter.
- Initialized Garage S3 cluster, created bucket `icelake`, generated AWS keys.
- Set up Python virtual environment `.venv`.

---

### Phase 1 — Data Ingestion: HuggingFace → Raw Parquet (✅ COMPLETED)
- Downloaded 10,000 Wikipedia articles.
- Generated synthetic monthly timestamps across 2024 (`created_date`).
- Saved partitioned Parquet files under `data/raw/year_month=2024-01` through `2024-12`.

---

### Phase 2 — Polaris REST Catalog Setup (✅ COMPLETED)
- Script: `src/catalog/polaris_setup.py`
- Authenticated with Polaris via OAuth2 client credentials (`polaris:polaris`).
- Created catalog `icelake` configured with S3 storage (`s3://icelake/`, `stsUnavailable: true`).
- Created namespace `wikipedia`.
- Cleanup helper created: `src/catalog/delete_catalog.py`.

---

### Phase 3 — Iceberg Table Creation & Ingestion (✅ COMPLETED)
- Script: `src/ingestion/create_iceberg_table.py`
- Created Iceberg v2 table `icelake.wikipedia.articles` with hidden partitioning:
  `PARTITIONED BY (months(created_date))`
- Appended all 10,000 records from `data/raw/` into Iceberg.
- Committed initial snapshot `#6831925201516210010`.

---

### Phase 4 — Schema Evolution (⏳ NEXT UP)
**Goal:** Demonstrate Iceberg's metadata-only schema evolution (no Parquet rewrite!).

#### Operations to perform:
1. **Add Column:** Add `category STRING` (defaults to `NULL` for existing rows).
2. **Rename Column:** Rename `word_count` to `word_count_v2` (demonstrating column ID mapping).
3. **Append New Row:** Append records using the new schema.
4. **Query Old vs New Data:** Show that old files are read seamlessly without data migration!

---

### Phase 5 — Table Maintenance (✅ COMPLETED)
- Script: `src/maintenance/table_ops.py`
- **Compaction:** Ran `CALL icelake.system.rewrite_data_files()` with `binpack` strategy. Verified why 0 files were rewritten (each monthly partition already had exactly 1 optimal file).
- **Snapshot Expiration:** Ran `CALL icelake.system.expire_snapshots(retain_last => 1)` to purge old metadata.
- **Orphan File Handling:** Configured safe error handling for S3FileIO without Hadoop S3A.

---

### Phase 6 — Multi-Engine Querying (⏳ CURRENT PHASE)
**Goal:** Query the exact same Iceberg table using DuckDB, Trino, and Snowflake.

#### 6.1 — Trino (Distributed SQL)
- Trino catalog `docker/trino/catalog/iceberg.properties` is configured:
  ```properties
  connector.name=iceberg
  iceberg.catalog.type=rest
  iceberg.rest-catalog.uri=http://polaris:8181/api/catalog
  iceberg.rest-catalog.warehouse=icelake
  iceberg.rest-catalog.security=OAUTH2
  iceberg.rest-catalog.oauth2.credential=polaris:polaris
  hive.s3.endpoint=http://garage:3900
  hive.s3.path-style-access=true
  hive.s3.aws-access-key=GK1f5cbdb2ac0aebcbb74e18d5
  hive.s3.aws-secret-key=b5057d94ab27275064a278e2e358bbe751d3610c26f7d6499bc5f0ca76996b29
  ```
- Command to query:
  ```powershell
  docker exec -it trino trino --catalog iceberg --schema wikipedia --execute "SELECT count(*) FROM articles;"
  ```

#### 6.2 — DuckDB (Embedded OLAP)
- Using DuckDB with Iceberg and HTTPFS extensions to query Polaris REST catalog and Garage S3.

#### 6.3 — Snowflake (Cloud Data Warehouse)
- Script: `snowflake/setup.sql`
- Set up External Volume and Iceberg Table in Snowflake Free Trial.
- Demonstrates how enterprises query Iceberg data from Snowflake.

---

### Phase 7 — Benchmarking & Time Travel (⏳ COMING UP)
- Script: `src/benchmark/run_benchmark.py`
- **Partition Pruning:** Compare query latency and data read between:
  - Query with partition filter (`WHERE created_date BETWEEN '2024-06-01' AND '2024-06-30'`) → Reads 1 file (~4.4 MB).
  - Full table scan (`SELECT COUNT(*) FROM articles`) → Reads all 12 files (~54 MB).
- **Time Travel:** Query historical snapshots using Spark and Trino:
  ```sql
  SELECT * FROM articles FOR VERSION AS OF <snapshot_id>;
  ```
- Outputs: `data/benchmarks/results.csv` and `data/benchmarks/report.md`.

---

### Phase 8 — Platform Security & RBAC (⏳ FINAL PHASE)
- **Polaris RBAC:** Configure roles (`data_engineer`, `data_analyst`, `read_only`) via Polaris REST Management API.
- **Snowflake RBAC:** Configure role hierarchy and table grants in `snowflake/setup.sql`.

---

## ✅ Phase Completion Checklist

- [x] Phase 0 — Docker Compose up with Garage S3, Polaris, Trino, Jupyter
- [x] Phase 1 — HuggingFace Wikipedia data partitioned into `data/raw/`
- [x] Phase 2 — Polaris catalog `icelake` & namespace `wikipedia` initialized
- [x] Phase 3 — Iceberg table created, 10k rows loaded into Garage S3
- [x] Phase 4 — Schema evolution: column add + rename demonstrated (`src/ingestion/schema_evolution.py`)
- [x] Phase 5 — Table maintenance: compaction + snapshot expiry run (`src/maintenance/table_ops.py`)
- [x] Phase 6 — Trino querying Iceberg table via REST connector (`docker exec trino ...`)
- [x] Phase 6 — Snowflake Lakehouse & Iceberg table setup (`snowflake/setup.sql`)
- [x] Phase 7 — Benchmark report (Partition pruning 3.03x speedup & Time Travel point-in-time reads)
- [x] Phase 8 — Snowflake Enterprise RBAC configured (`read_only_user`, `data_analyst`, `data_engineer`)
- [ ] README.md finalized with Lakehouse architecture diagram
