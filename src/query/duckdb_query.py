import os
import duckdb
from dotenv import load_dotenv

load_dotenv()

def query_with_duckdb():
    print("=" * 60)
    print("[DUCKDB] (4th QUERY ENGINE) - EMBEDDED OLAP DEMONSTRATION")
    print("=" * 60)

    con = duckdb.connect()

    # Load S3 and Iceberg extensions
    print("\n[1] Loading DuckDB HTTPFS & Iceberg Extensions...")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    try:
        con.execute("INSTALL iceberg; LOAD iceberg;")
    except Exception:
        pass

    # Configure Garage S3 settings
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://localhost:3900").replace("http://", "")
    aws_key = os.getenv("AWS_ACCESS_KEY_ID", "GK1f5cbdb2ac0aebcbb74e18d5")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "b5057d94ab27275064a278e2e358bbe751d3610c26f7d6499bc5f0ca76996b29")

    con.execute(f"""
        SET s3_endpoint = '{s3_endpoint}';
        SET s3_access_key_id = '{aws_key}';
        SET s3_secret_access_key = '{aws_secret}';
        SET s3_url_style = 'path';
        SET s3_use_ssl = false;
    """)
    print("[OK] S3 Configuration Applied to DuckDB!")

    # Query 1: Querying S3 directly via DuckDB with schema union
    print("\n[2] Querying Parquet Data on Garage S3 via DuckDB (with Schema Evolution handling):")
    df_s3 = con.execute("""
        SELECT 
            count(*) AS total_articles,
            min(created_date) AS min_date,
            max(created_date) AS max_date,
            count(category) AS articles_with_category,
            count(word_count_v2) AS articles_with_word_count_v2
        FROM read_parquet('s3://icelake/wikipedia/articles/data/*/*.parquet', union_by_name=True)
    """).df()
    print(df_s3)

    # Query 2: Fast In-Memory Aggregations (OLAP)
    print("\n[3] Monthly Distribution Analytics (Fast In-Process OLAP):")
    monthly_df = con.execute("""
        SELECT 
            strftime(created_date, '%Y-%m') AS month,
            count(*) AS article_count,
            count(category) AS category_tagged_count
        FROM read_parquet('s3://icelake/wikipedia/articles/data/*/*.parquet', union_by_name=True)
        GROUP BY 1
        ORDER BY 1
    """).df()
    print(monthly_df)

    print("\n" + "=" * 60)
    print("[SUCCESS] DUCKDB MULTI-ENGINE QUERY COMPLETED SUCCESSFULLY!")
    print("=" * 60)

if __name__ == "__main__":
    query_with_duckdb()
