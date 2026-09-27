from datasets import load_dataset
import pandas as pd
import os           

def download_dataset(output_dir="data/raw", n_rows=10000):
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Downloading {n_rows} rows from HuggingFace Wikipedia...")
    ds = load_dataset(
        "wikimedia/wikipedia",
        "20231101.en",
        split=f"train[:{n_rows}]",
        trust_remote_code=True
    )
    
    df = ds.to_pandas()
    df = df[["id", "title", "text", "url"]]
    
    # Fake timestamp for partitioning demo
    df["created_date"] = (pd.to_datetime("2024-01-01") + pd.to_timedelta(
        df.index % 365, unit="D"
    )).astype("datetime64[us]")   # microsecond precision — Spark-compatible
    
    df["word_count"] = df["text"].str.split().str.len()
    df["year_month"] = df["created_date"].dt.to_period("M").astype(str)
    
    # Save partitioned by month
    saved = 0
    for ym, group in df.groupby("year_month"):
        path = f"{output_dir}/year_month={ym}/data.parquet"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        group.drop(columns=["year_month"]).to_parquet(path, index=False)
        saved += len(group)
    
    print(f"✅ Saved {saved} rows across {df['year_month'].nunique()} monthly partitions")
    print(f"📁 Location: {output_dir}/")
    return df

if __name__ == "__main__":
    download_dataset()