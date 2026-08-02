from pyspark.sql.functions import col, when, trim

df = spark.table("network_anomaly_detection.bronze.flows")
before = df.count()

# Infinity -> 0 (Flow Duration = 0 causes division by zero)
for c in ["flow_bytes_s", "flow_packets_s"]:
    df = df.withColumn(c, when(col(c).isin(float("inf"), float("-inf")), 0.0).otherwise(col(c)))

# init_win_bytes_* is NOT touched — -1 is a valid CICFlowMeter sentinel value, not an error

# Genuine negative value glitches — negligible row count, safe to drop
negative_glitch_cols = [
    "flow_duration", "flow_bytes_s", "flow_packets_s",
    "flow_iat_mean", "flow_iat_max", "flow_iat_min", "fwd_iat_min",
    "fwd_header_length34", "bwd_header_length", "fwd_header_length55",
    "min_seg_size_forward",
]
for c in negative_glitch_cols:
    df = df.filter(col(c) >= 0)
after_negative = df.count()

# Deduplication
df = df.dropDuplicates([c for c in df.columns if c not in ("_source_file", "_ingested_at")])
after_dedup = df.count()

# Label cleaning: match on the readable part, not the corrupted character
df = df.withColumn(
    "Label",
    when(col("Label").startswith("Web Attack") & col("Label").contains("Brute"), "Web Attack - Brute Force")
    .when(col("Label").startswith("Web Attack") & col("Label").contains("XSS"), "Web Attack - XSS")
    .when(col("Label").startswith("Web Attack") & col("Label").contains("Sql"), "Web Attack - SQL Injection")
    .otherwise(trim(col("Label")))
)

print(f"Bronze: {before} -> after negative filter: {after_negative} (-{before - after_negative}) -> after dedup: {after_dedup}")
df.write.mode("overwrite").saveAsTable("network_anomaly_detection.silver.flows_clean")