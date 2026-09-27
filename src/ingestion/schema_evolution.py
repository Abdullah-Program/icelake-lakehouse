import os
from datetime import date
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

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
        .appName("IceLake-SchemaEvolution")
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

def demonstrate_schema_evolution(spark):
    print("=" * 60)
    print("🚀 APACHE ICEBERG SCHEMA EVOLUTION DEMONSTRATION")
    print("=" * 60)

    # 1. Print Initial Schema
    print("\n📋 1. Current Table Schema (Before Evolution):")
    spark.sql("DESCRIBE TABLE icelake.wikipedia.articles").show(truncate=False)

    # 2. Add New Column: category STRING
    print("\n➕ 2. Evolving Schema: Adding 'category STRING' column...")
    spark.sql("ALTER TABLE icelake.wikipedia.articles ADD COLUMNS (category STRING)")
    print("✅ Column 'category' added to table metadata (Zero data rewrite!)")

    # 3. Rename Column: word_count -> word_count_v2
    print("\n🔄 3. Evolving Schema: Renaming 'word_count' to 'word_count_v2'...")
    spark.sql("ALTER TABLE icelake.wikipedia.articles RENAME COLUMN word_count TO word_count_v2")
    print("✅ Column renamed using Iceberg Field ID tracking (Zero data rewrite!)")

    # 4. Print Updated Schema
    print("\n📋 4. Updated Table Schema (After Evolution):")
    spark.sql("DESCRIBE TABLE icelake.wikipedia.articles").show(truncate=False)

    # 5. Append New Records using the New Schema
    print("\n📝 5. Appending new records matching the evolved schema...")
    new_data = [
        (
            "evolved-101",
            "Apache Iceberg Schema Evolution",
            "Schema evolution in Iceberg allows column additions, renames, and drops without rewriting Parquet data.",
            "https://en.wikipedia.org/wiki/Apache_Iceberg",
            date(2024, 6, 15),
            185,
            "Lakehouse Technology"
        ),
        (
            "evolved-102",
            "Multi-Engine Interoperability",
            "Iceberg tables can be read simultaneously by Spark, Trino, DuckDB, and Snowflake using the same metadata.",
            "https://en.wikipedia.org/wiki/Data_lakehouse",
            date(2024, 7, 20),
            210,
            "Cloud Architecture"
        )
    ]
    
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("title", StringType(), True),
        StructField("text", StringType(), True),
        StructField("url", StringType(), True),
        StructField("created_date", DateType(), True),
        StructField("word_count_v2", IntegerType(), True),
        StructField("category", StringType(), True)
    ])

    new_df = spark.createDataFrame(new_data, schema=schema)
    new_df.writeTo("icelake.wikipedia.articles").append()
    print("✅ New records appended successfully!")

    # 6. Verify Old vs New Data Compatibility
    print("\n🔍 6. Querying Old Data (Notice: category is NULL, word_count_v2 has original values):")
    spark.sql("""
        SELECT id, title, created_date, word_count_v2, category 
        FROM icelake.wikipedia.articles 
        WHERE category IS NULL 
        LIMIT 3
    """).show(truncate=False)

    print("\n🔍 7. Querying Newly Appended Data (Notice: category and word_count_v2 populated):")
    spark.sql("""
        SELECT id, title, created_date, word_count_v2, category 
        FROM icelake.wikipedia.articles 
        WHERE category IS NOT NULL
    """).show(truncate=False)

    # 8. Check Snapshots
    print("\n📜 8. Table Snapshots:")
    spark.sql("""
        SELECT committed_at, snapshot_id, operation, summary['added-records'] as added_records, summary['total-records'] as total_records
        FROM icelake.wikipedia.articles.snapshots
        ORDER BY committed_at DESC
    """).show(truncate=False)

    print("\n" + "=" * 60)
    print("🎉 SCHEMA EVOLUTION COMPLETED SUCCESSFULLY!")
    print("=" * 60)

if __name__ == "__main__":
    spark = get_spark()
    demonstrate_schema_evolution(spark)
    spark.stop()
