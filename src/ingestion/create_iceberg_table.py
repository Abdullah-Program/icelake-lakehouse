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
CLIENT_ID = os.getenv("POLARIS_CLIENT_ID")
CLIENT_SECRET = os.getenv("POLARIS_CLIENT_SECRET")
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "icelake")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "GK1f5cbdb2ac0aebcbb74e18d5")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "b5057d94ab27275064a278e2e358bbe751d3610c26f7d6499bc5f0ca76996b29")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://garage:3900")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

ICEBERG_VERSION = "1.6.1"
SPARK_VERSION = "3.5"

def create_spark_session():
    return (
        SparkSession.builder
        .appName("IceLake-Ingestion")
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
        .config("spark.sql.shuffle.partitions", "4")
        .master("local[2]")
        .getOrCreate()
    )

def create_table_and_load(spark):
    print("Creating Iceberg table...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS icelake.wikipedia.articles (
            id           STRING,
            title        STRING,
            text         STRING,
            url          STRING,
            created_date DATE,
            word_count   INT
        )
        USING iceberg
        PARTITIONED BY (months(created_date))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)
    print("✅ Table created with hidden partitioning")

    raw_df = spark.read.parquet("data/raw/")
    raw_df = raw_df.select("id", "title", "text", "url", "created_date", "word_count")
    print(f"Read {raw_df.count()} rows from raw parquet")

    raw_df.writeTo("icelake.wikipedia.articles").append()
    print("✅ Data loaded into Iceberg table")

    spark.sql("""
        SELECT COUNT(*) as total_rows, MIN(created_date) as min_date, MAX(created_date) as max_date
        FROM icelake.wikipedia.articles
    """).show()

    print("\n--- Snapshots ---")
    spark.sql("SELECT * FROM icelake.wikipedia.articles.snapshots").show(truncate=False)

if __name__ == "__main__":
    spark = create_spark_session()
    create_table_and_load(spark)
    spark.stop()