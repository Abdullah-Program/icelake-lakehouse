# 🏗️ IceLake Local — Production-Grade Open Lakehouse

[![Apache Iceberg](https://img.shields.io/badge/Table_Format-Apache_Iceberg_1.6.1-blue.svg)](https://iceberg.apache.org/)
[![Apache Polaris](https://img.shields.io/badge/REST_Catalog-Apache_Polaris-00A4E4.svg)](https://polaris.apache.org/)
[![Garage S3](https://img.shields.io/badge/Object_Storage-Garage_S3-orange.svg)](https://garagehq.deuxfleurs.fr/)
[![Apache Spark](https://img.shields.io/badge/ETL_Engine-PySpark_3.5-E25A1C.svg)](https://spark.apache.org/)
[![Trino](https://img.shields.io/badge/Query_Engine-Trino_475-DD00A1.svg)](https://trino.io/)
[![Snowflake](https://img.shields.io/badge/Data_Cloud-Snowflake-29B5E8.svg)](https://www.snowflake.com/)

An end-to-end, zero-cost, enterprise-grade Open Lakehouse demonstrating modern data platform engineering. Designed to mirror real-world **AWS Data Engineer (Open Data Platform)** responsibilities with **100% free, local tools**.

---

## 🏛️ High-Level Architecture

```
                                  ┌────────────────────────────────────────┐
                                  │   Multi-Engine Analytical Layer       │
                                  ├──────────────┬──────────────┬──────────┤
                                  │   PySpark    │    Trino     │ Snowflake│
                                  │  (ETL & Ops) │  (Ad-hoc BI) │  (Cloud) │
                                  └──────▲───────┴──────▲───────┴────▲─────┘
                                         │              │            │
                         OAuth2 Metadata │              │ Metadata   │ RBAC /
                           Pointers      │              │ Resolution │ External Vol
                                  ┌──────┴──────────────┴────────────┴─────┐
                                  │      Apache Polaris REST Catalog       │
                                  │  (Centralized Metadata, Namespaces,    │
                                  │   Table Commits & Snapshot History)    │
                                  └──────────────────▲─────────────────────┘
                                                     │
                               Manifest & Parquet I/O│ (S3FileIO / REST API)
                                                     │
                                  ┌──────────────────┴─────────────────────┐
                                  │        Garage S3 Object Storage        │
                                  │          s3://icelake/articles/        │
                                  │    • metadata/*.json (Table Schema)    │
                                  │    • metadata/*.avro (Manifest Lists)  │
                                  │    • data/*/*.parquet (Monthly Part.)  │
                                  └────────────────────────────────────────┘
```

---

## 🌟 Key Engineering Highlights

### 1. Object Storage Parity (Garage S3 Upgrade)
Enterprise lakehouses never run on raw local file paths (`file:///`)—they run on **Object Storage (Amazon S3 / GCS)** with AWS Signature v4 and `S3FileIO`.  
We deployed **Garage S3** (`garage:3900`) inside Docker:
- Provides a genuine, production-parity S3 endpoint (`s3://icelake/`) with AWS Access Keys and Secret Keys.
- Allows Spark and Trino to use native S3 client implementations (`S3FileIO`), exactly mirroring AWS production.

### 2. Hidden Partitioning (`months(created_date)`)
- Traditional Hive partitioning requires hardcoding physical partition paths (`year_month=2024-06`), leading to partition drift and query errors.
- Iceberg's **hidden partitioning** derives the partition directly from the business column `created_date`.
- Users query naturally with `WHERE created_date BETWEEN '2024-06-01' AND '2024-06-30'`. Iceberg's manifest metadata automatically skips 11 out of 12 data files, yielding a **3.03x speedup**!

### 3. Metadata-Only Schema Evolution
Demonstrated seamless column addition and rename without rewriting existing Parquet files:
- **Added Column:** `category STRING` (existing rows safely evaluate to `NULL`).
- **Renamed Column:** `word_count` ➡️ `word_count_v2` (tracked by unique Iceberg Field IDs; no physical file modification).
- **Zero Downtime:** Trino and Spark immediately query old and new records in a single unified schema.

### 4. Table Maintenance (Compaction & Snapshot Expiry)
- **File Compaction:** Executed `CALL icelake.system.rewrite_data_files()` with the `binpack` strategy to optimize small files into target sizes.
- **Snapshot Expiration:** Executed `CALL icelake.system.expire_snapshots(retain_last => 1)` to prune unreferenced data files and maintain lightweight metadata.

### 5. Time Travel & Point-in-Time Auditing
- Historical state is immutable. Querying `VERSION AS OF <snapshot_id>` allows reproducible analytics, historical backfills, and audit tracking.

### 6. Cloud Snowflake Integration & Enterprise RBAC
- Defined Lakehouse table schemas and configured role hierarchies:
  - `read_only_user`: Restricted strictly to `SELECT` queries on the Lakehouse table.
  - `data_analyst`: Granted `INSERT, UPDATE, DELETE` permissions.
  - `data_engineer`: Granted administrative `ALL` schema control.

---

## 📊 Benchmark Results

| Engine | Query Type | Latency (s) | Files Scanned | Architectural Impact |
|---|---|---|---|---|
| **PySpark 3.5** | Full Table Scan (12 Partitions) | `2.407s` | `12` | Reads all 12 monthly Parquet files (~54MB) |
| **PySpark 3.5** | Partition Pruned (`June 2024`) | `0.793s` | `1` | **3.03x Faster**; skipped 11 files via metadata |
| **Trino 475** | Full Table Scan (12 Partitions) | `0.201s` | `12` | Distributed vectorized scan |
| **Trino 475** | Partition Pruned (`June 2024`) | `0.126s` | `1` | Sub-second latency; evaluated Iceberg manifests |

---

## 📁 Repository Structure

```
icelake-local/
├── docker/
│   ├── docker-compose.yml          # Garage S3, Polaris, Trino, Jupyter
│   ├── garage/garage.toml          # S3 storage node configuration
│   └── trino/catalog/
│       └── iceberg.properties      # Trino REST Catalog connector settings
├── src/
│   ├── ingestion/
│   │   ├── hf_to_iceberg.py        # HuggingFace Wikipedia download & partitioner
│   │   ├── create_iceberg_table.py # Iceberg table creation & initial S3 ingestion
│   │   └── schema_evolution.py     # Column addition, rename, & verification
│   ├── catalog/
│   │   ├── polaris_setup.py        # Bootstraps Polaris catalog & namespace
│   │   └── delete_catalog.py       # Teardown / cleanup helper
│   ├── maintenance/
│   │   └── table_ops.py            # Compaction & snapshot expiration
│   └── benchmark/
│       └── run_benchmark.py        # Latency benchmark & time-travel validation
├── snowflake/
│   └── setup.sql                   # Snowflake Lakehouse tables & RBAC script
├── data/
│   ├── raw/                        # 10k Wikipedia articles across 12 months
│   └── benchmarks/
│       ├── results.csv             # Execution times across engines
│       └── report.md               # Detailed markdown benchmark report
├── requirements.txt                # Python dependencies
└── README.md                       # Project documentation
```

---

## ⚡ Quick Start Guide

### 1. Launch Docker Infrastructure
```powershell
docker compose -f docker\docker-compose.yml up -d
```

### 2. Bootstrap Polaris REST Catalog
```powershell
docker exec jupyter bash -c "cd /home/jovyan/work && python src/catalog/polaris_setup.py"
```

### 3. Create & Load Iceberg Table
```powershell
docker exec jupyter bash -c "cd /home/jovyan/work && python src/ingestion/create_iceberg_table.py"
```

### 4. Execute Schema Evolution
```powershell
docker exec jupyter bash -c "cd /home/jovyan/work && python src/ingestion/schema_evolution.py"
```

### 5. Run Table Maintenance (Compaction & Expiry)
```powershell
docker exec jupyter bash -c "cd /home/jovyan/work && python src/maintenance/table_ops.py"
```

### 6. Run Benchmark & Time Travel Validation
```powershell
docker exec jupyter bash -c "cd /home/jovyan/work && python src/benchmark/run_benchmark.py"
```

### 7. Query via Trino CLI
```powershell
docker exec -it trino trino --catalog iceberg --schema wikipedia --execute "SELECT title, word_count_v2, category FROM articles WHERE category IS NOT NULL;"
```

---

## 🎯 Interview Talking Points

- **Why Iceberg over Hive tables?**  
  Iceberg replaces directory-based table tracking with file-based metadata trees. It provides ACID transactions, snapshot isolation, hidden partitioning, metadata-only schema evolution, and prevents corrupted tables from concurrent writes.
- **Why Polaris REST Catalog?**  
  Polaris implements the open Apache Iceberg REST Catalog spec. It decouples the storage layer from compute engines, allowing Spark, Trino, Flink, and Snowflake to share the exact same catalog without vendor lock-in.
- **How does this map to AWS Glue in Production?**  
  AWS Glue ETL jobs are serverless Apache Spark jobs. In production, AWS Glue Data Catalog natively acts as an Iceberg catalog, S3 stores the Parquet/Avro files with `S3FileIO`, and Amazon Athena or Snowflake queries the data through IAM Role integrations.
