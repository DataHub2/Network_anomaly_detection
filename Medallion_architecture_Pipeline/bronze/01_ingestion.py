from pyspark.sql.functions import current_timestamp, col

# 1. Define the input volume path
volume_path = "/Volumes/network_anomaly_detection/bronze/raw/"

# 2. Read all raw CSV files in a single pass
raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(volume_path + "*.csv")
)

# 3. Clean column names to satisfy Delta Lake & SQL standards:
# - Strip leading/trailing whitespaces (.strip())
# - Convert characters to lower case (.lower())
# - Replace spaces, slashes, and hyphens with underscores
clean_cols = [
    col(f"`{c}`").alias(
        c.strip().lower().replace(" ", "_").replace("/", "_").replace("-", "_")
    ) 
    for c in raw_df.columns
]

# 4. Apply clean column names and append Unity Catalog metadata
bronze_df = (
    raw_df.select(*clean_cols)
    .withColumn("_source_file", col("_metadata.file_name"))
    .withColumn("_ingested_at", current_timestamp())
)

# 5. Write to Bronze Delta Table in Unity Catalog
bronze_df.write.mode("overwrite").saveAsTable("network_anomaly_detection.bronze.flows")

print("Ingestion complete! Table 'network_anomaly_detection.bronze.flows' successfully created.")