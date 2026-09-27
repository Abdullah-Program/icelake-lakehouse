# 📊 IceLake Local — Query & Partition Pruning Benchmark Report
**Generated:** 2026-09-27 20:50:53 UTC  
**Storage:** Garage S3 (`s3://icelake/`)  
**Catalog:** Apache Polaris REST  
**Dataset:** 10,000+ Wikipedia Articles across 12 Monthly Partitions (`months(created_date)`)

---

## 📈 Benchmark Results Table

| Engine | Query Type | Latency (s) | Files Scanned | Efficiency Notes |
|---|---|---|---|---|
| **PySpark 3.5** | Full Table Scan (12 Partitions) | `2.407s` | `12` | Scanned all 12 monthly Parquet files (~54MB) |
| **PySpark 3.5** | Partition Pruned (1 Partition - June 2024) | `0.793s` | `1` | Hidden partitioning months(created_date) skipped 11 files |
| **Trino 475** | Full Table Scan (12 Partitions) | `0.201s` | `12` | Distributed SQL full table scan |
| **Trino 475** | Partition Pruned (1 Partition - June 2024) | `0.126s` | `1` | Trino connector evaluated Iceberg partition metadata |

---

## 💡 Key Architectural Insights

1. **Hidden Partitioning Benefit (`months(created_date)`):**
   - In traditional Hive tables, queries must explicitly filter on partition strings (`WHERE year_month = '2024-06'`), leaking storage details to users.
   - With Iceberg's **hidden partitioning**, users query naturally on `created_date BETWEEN '2024-06-01' AND '2024-06-30'`.
   - The query engine reads the manifest metadata and immediately skips **11 out of 12 data files**, cutting I/O and latency significantly.

2. **Time Travel Durability:**
   - Historical state is immutable. Querying `VERSION AS OF <initial_snapshot>` returns the exact point-in-time dataset before schema evolution or new appends.

3. **Multi-Engine Consistency:**
   - Both Spark and Trino execute against the exact same Parquet files on Garage S3, coordinated through Polaris REST Catalog without data duplication.
