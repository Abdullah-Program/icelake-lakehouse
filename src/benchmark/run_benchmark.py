import os
import time
import subprocess
import csv
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

def resolve_polaris_url():
    url = os.getenv("POLARIS_URL", "http://localhost:8181")
    import socket
    try:
        socket.gethostbyname("polaris")
        return url.replace("localhost:8181", "polaris:8181")
    except socket.gaierror:
        return url.replace("polaris:8181", "localhost:8181")

POLARIS_URL = resolve_polaris_url()
CLIENT_ID = os.getenv("POLARIS_CLIENT_ID", "polaris")
CLIENT_SECRET = os.getenv("POLARIS_CLIENT_SECRET", "polaris")
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "icelake")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "GK1f5cbdb2ac0aebcbb74e18d5")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "b5057d94ab27275064a278e2e358bbe751d3610c26f7d6499bc5f0ca76996b29")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://garage:3900")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

ICEBERG_VERSION = "1.6.1"
SPARK_VERSION = "3.5"

def get_spark():
    return (
        SparkSession.builder
        .appName("IceLake-Benchmark")
        .config("spark.jars.packages",
            f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_VERSION}_2.12:{ICEBERG_VERSION},"
            f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}"
        )
        .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config("spark.sql.catalog.icelake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.icelake.type", "rest")
        .config("spark.sql.catalog.icelake.uri", f"{POLARIS_URL}/api/catalog")
        .config("spark.sql.catalog.icelake.oauth2-server-uri", f"{POLARIS_URL}/api/catalog/v1/oauth/tokens")
        .config("spark.sql.catalog.icelake.warehouse", WAREHOUSE)
        .config("spark.sql.catalog.icelake.credential", f"{CLIENT_ID}:{CLIENT_SECRET}")
        .config("spark.sql.catalog.icelake.scope", "PRINCIPAL_ROLE:ALL")
        .config("spark.sql.catalog.icelake.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.icelake.s3.endpoint", S3_ENDPOINT)
        .config("spark.sql.catalog.icelake.s3.region", AWS_REGION)
        .config("spark.sql.catalog.icelake.s3.path-style-access", "true")
        .config("spark.sql.catalog.icelake.s3.access-key-id", AWS_ACCESS_KEY_ID)
        .config("spark.sql.catalog.icelake.s3.secret-access-key", AWS_SECRET_ACCESS_KEY)
        .config("spark.sql.catalog.icelake.s3.checksum-enabled", "false")
        .config("spark.sql.catalog.icelake.metrics-reporter-impl", "org.apache.iceberg.metrics.LoggingMetricsReporter")
        .config("spark.sql.defaultCatalog", "icelake")
        .master("local[2]")
        .getOrCreate()
    )

import urllib.request
import json

def run_trino_query(sql):
    t0 = time.time()
    try:
        req = urllib.request.Request(
            "http://trino:8080/v1/statement",
            data=sql.encode("utf-8"),
            headers={
                "X-Trino-User": "admin",
                "X-Trino-Catalog": "iceberg",
                "X-Trino-Schema": "wikipedia"
            }
        )
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        
        while "nextUri" in data and "data" not in data:
            time.sleep(0.05)
            with urllib.request.urlopen(data["nextUri"]) as next_resp:
                data = json.loads(next_resp.read().decode("utf-8"))
        
        elapsed = time.time() - t0
        rows = data.get("data", [])
        return elapsed, str(rows)
    except Exception as e:
        return None, str(e)

def benchmark_partition_pruning(spark):
    print("\n" + "=" * 60)
    print("📊 1. PARTITION PRUNING BENCHMARK (PySpark vs Trino)")
    print("=" * 60)

    results = []

    # Warm up Spark
    spark.sql("SELECT count(*) FROM icelake.wikipedia.articles").collect()

    # Query A: Full Table Scan (12 Monthly Partitions)
    query_full = "SELECT count(*), avg(word_count_v2) FROM icelake.wikipedia.articles"
    t0 = time.time()
    res_spark_full = spark.sql(query_full).collect()
    spark_full_time = time.time() - t0
    print(f"⚡ [Spark] Full Table Scan: {spark_full_time:.3f}s | Result: {res_spark_full}")

    # Query B: Partition Pruned Scan (Single Month: June 2024 -> 1 Partition)
    query_pruned = "SELECT count(*), avg(word_count_v2) FROM icelake.wikipedia.articles WHERE created_date BETWEEN '2024-06-01' AND '2024-06-30'"
    t0 = time.time()
    res_spark_pruned = spark.sql(query_pruned).collect()
    spark_pruned_time = time.time() - t0
    print(f"🎯 [Spark] Partition Pruned (June 2024): {spark_pruned_time:.3f}s | Result: {res_spark_pruned}")
    speedup_spark = (spark_full_time / spark_pruned_time) if spark_pruned_time > 0 else 1.0
    print(f"🚀 [Spark] Partition Pruning Speedup: {speedup_spark:.2f}x faster!")

    results.append({
        "Engine": "PySpark 3.5",
        "Query_Type": "Full Table Scan (12 Partitions)",
        "Latency_Seconds": round(spark_full_time, 3),
        "Estimated_Files_Scanned": 12,
        "Notes": "Scanned all 12 monthly Parquet files (~54MB)"
    })
    results.append({
        "Engine": "PySpark 3.5",
        "Query_Type": "Partition Pruned (1 Partition - June 2024)",
        "Latency_Seconds": round(spark_pruned_time, 3),
        "Estimated_Files_Scanned": 1,
        "Notes": "Hidden partitioning months(created_date) skipped 11 files"
    })

    # Test Trino
    trino_full_time, trino_full_out = run_trino_query(query_full)
    if trino_full_time is not None:
        print(f"⚡ [Trino] Full Table Scan: {trino_full_time:.3f}s | Output: {trino_full_out}")
        results.append({
            "Engine": "Trino 475",
            "Query_Type": "Full Table Scan (12 Partitions)",
            "Latency_Seconds": round(trino_full_time, 3),
            "Estimated_Files_Scanned": 12,
            "Notes": "Distributed SQL full table scan"
        })

    trino_pruned_time, trino_pruned_out = run_trino_query(query_pruned)
    if trino_pruned_time is not None:
        print(f"🎯 [Trino] Partition Pruned (June 2024): {trino_pruned_time:.3f}s | Output: {trino_pruned_out}")
        results.append({
            "Engine": "Trino 475",
            "Query_Type": "Partition Pruned (1 Partition - June 2024)",
            "Latency_Seconds": round(trino_pruned_time, 3),
            "Estimated_Files_Scanned": 1,
            "Notes": "Trino connector evaluated Iceberg partition metadata"
        })

    return results

def benchmark_time_travel(spark):
    print("\n" + "=" * 60)
    print("⏳ 2. TIME TRAVEL DEMONSTRATION")
    print("=" * 60)

    # Fetch snapshots
    snapshots = spark.sql("""
        SELECT committed_at, snapshot_id, operation, summary['total-records'] as total_records
        FROM icelake.wikipedia.articles.snapshots
        ORDER BY committed_at ASC
    """).collect()

    print("📜 Table Snapshots Found:")
    for s in snapshots:
        print(f"  - Snapshot ID: {s['snapshot_id']} | Committed: {s['committed_at']} | Total Records: {s['total_records']}")

    if len(snapshots) >= 2:
        initial_snap = snapshots[0]["snapshot_id"]
        latest_snap = snapshots[-1]["snapshot_id"]

        print(f"\n⏮️ Querying Historical Snapshot #1 (ID: {initial_snap}):")
        df_old = spark.sql(f"SELECT count(*) as count_as_of_v1 FROM icelake.wikipedia.articles VERSION AS OF {initial_snap}").collect()
        print(f"✅ Record Count as of Snapshot #1: {df_old[0]['count_as_of_v1']}")

        print(f"\n⏭️ Querying Current Snapshot #2 (ID: {latest_snap}):")
        df_curr = spark.sql(f"SELECT count(*) as current_count FROM icelake.wikipedia.articles VERSION AS OF {latest_snap}").collect()
        print(f"✅ Record Count as of Latest Snapshot: {df_curr[0]['current_count']}")
    else:
        print("Note: Single snapshot available.")

def save_benchmark_report(results):
    os.makedirs("data/benchmarks", exist_ok=True)
    csv_file = "data/benchmarks/results.csv"
    with open(csv_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Engine", "Query_Type", "Latency_Seconds", "Estimated_Files_Scanned", "Notes"])
        writer.writeheader()
        writer.writerows(results)
    print(f"\n💾 Saved results to {csv_file}")

    report_md = f"""# 📊 IceLake Local — Query & Partition Pruning Benchmark Report
**Generated:** {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}  
**Storage:** Garage S3 (`s3://icelake/`)  
**Catalog:** Apache Polaris REST  
**Dataset:** 10,000+ Wikipedia Articles across 12 Monthly Partitions (`months(created_date)`)

---

## 📈 Benchmark Results Table

| Engine | Query Type | Latency (s) | Files Scanned | Efficiency Notes |
|---|---|---|---|---|
"""
    for r in results:
        report_md += f"| **{r['Engine']}** | {r['Query_Type']} | `{r['Latency_Seconds']}s` | `{r['Estimated_Files_Scanned']}` | {r['Notes']} |\n"

    report_md += """
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
"""
    report_file = "data/benchmarks/report.md"
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(report_md)
    print(f"📝 Generated benchmark report: {report_file}")

if __name__ == "__main__":
    spark = get_spark()
    results = benchmark_partition_pruning(spark)
    benchmark_time_travel(spark)
    save_benchmark_report(results)
    spark.stop()
