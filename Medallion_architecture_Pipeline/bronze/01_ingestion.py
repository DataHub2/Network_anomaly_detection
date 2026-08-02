from pyspark.sql.functions import current_timestamp, col
from collections import Counter

volume_path = "/Volumes/network_anomaly_detection/bronze/raw/"

raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(volume_path + "*.csv")
)

clean_cols = [
    col(f"`{c}`").alias(
        c.strip().lower().replace(" ", "_").replace("/", "_").replace("-", "_")
    )
    for c in raw_df.columns
]

bronze_df = (
    raw_df
    .withColumn("_source_file", col("_metadata.file_name"))
    .withColumn("_ingested_at", current_timestamp())
    .select(*clean_cols, "_source_file", "_ingested_at")
)

dupes = [name for name, n in Counter(bronze_df.columns).items() if n > 1]
if dupes:
    print(f"Warning — dubblerade kolumnnamn: {dupes}")

bronze_df.write.mode("overwrite").saveAsTable("network_anomaly_detection.bronze.flows")

files_found = bronze_df.select("_source_file").distinct().count()
print(f"Done: {bronze_df.count()} rows, {len(bronze_df.columns)} kolumner, {files_found} source files")