import os
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
        .appName("IceLake-Maintenance")
        .config("spark.jars.packages",
            f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_VERSION}_2.12:{ICEBERG_VERSION},"
            f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}"
        )
        .config("spark.sql.catalog.icelake.oauth2-server-uri", f"{POLARIS_URL}/api/catalog/v1/oauth/tokens")

        .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config("spark.sql.catalog.icelake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.icelake.type", "rest")
        .config("spark.sql.catalog.icelake.uri", f"{POLARIS_URL}/api/catalog")
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

def show_snapshots(spark):
    print("\n--- Current Snapshots ---")
    spark.sql("""
        SELECT 
            committed_at, 
            snapshot_id, 
            operation, 
            summary['total-data-files'] AS total_files,
            summary['total-records'] AS total_records
        FROM icelake.wikipedia.articles.snapshots
        ORDER BY committed_at ASC
    """).show(truncate=False)

def compact_table(spark):
    print("\n🧹 1. Running Compaction (rewrite_data_files)...")
    # Setting min-input-files to '1' forces Iceberg to rewrite and optimize each partition's files
    result = spark.sql("""
        CALL icelake.system.rewrite_data_files(
            table => 'wikipedia.articles',
            strategy => 'binpack',
            options => map('min-input-files', '1')
        )
    """)
    result.show(truncate=False)

def expire_snapshots(spark):
    print("\n⏳ 2. Expiring Old Snapshots (Retaining Only Latest 1)...")
    result = spark.sql("""
        CALL icelake.system.expire_snapshots(
            table => 'wikipedia.articles',
            retain_last => 1
        )
    """)
    result.show(truncate=False)

def remove_orphan_files(spark):
    print("\n🗑️ 3. Checking for Orphan Files...")
    try:
        result = spark.sql("""
            CALL icelake.system.remove_orphan_files(
                table => 'wikipedia.articles'
            )
        """)
        result.show(truncate=False)
    except Exception as e:
        print("ℹ️ Note: Orphan file scanner requires Hadoop S3A connector. Skipped for clean S3FileIO table.")

if __name__ == "__main__":
    spark = get_spark()
    
    # Step A: View snapshots BEFORE maintenance
    print("=== BEFORE MAINTENANCE ===")
    show_snapshots(spark)
    
    # Step B: Run Compaction (This creates a new 'replace' snapshot)
    compact_table(spark)
    
    # Step C: View snapshots AFTER compaction (Notice 2 snapshots now!)
    print("=== AFTER COMPACTION ===")
    show_snapshots(spark)
    
    # Step D: Expire old snapshot (Notice how snapshot #1 is safely pruned!)
    expire_snapshots(spark)
    
    # Step E: View snapshots AFTER expiration (Only the fresh compacted snapshot remains)
    print("=== AFTER EXPIRATION ===")
    show_snapshots(spark)
    
    # Step F: Orphan cleanup
    remove_orphan_files(spark)
    
    spark.stop()
