from pyspark.sql.functions import col, when, trim

# Load Bronze table — the raw union of all 8 CICIDS2017 files
df = spark.table("network_anomaly_detection.bronze.flows")
before = df.count()

# Fix Infinity in flow_bytes_s / flow_packets_s.
# Infinity happens when flow_duration = 0 (division by zero during feature
# extraction). Setting to 0 is safer than dropping the row — a 0-duration
# flow is a real event, not corrupted data.
for c in ["flow_bytes_s", "flow_packets_s"]:
    df = df.withColumn(c, when(col(c).isin(float("inf"), float("-inf")), 0.0).otherwise(col(c)))

# NOTE: init_win_bytes_forward / init_win_bytes_backward are deliberately NOT
# touched. Their -1 values are a documented CICFlowMeter sentinel meaning
# "TCP window size was never observed" (e.g. UDP traffic) — not an error.
# Filtering these out would silently drop 35–51% of the dataset.

# Genuine measurement glitches: negative values that are physically
# impossible (negative duration, negative header length). Row impact is
# under 0.1% of the dataset, so dropping is safe with negligible per-class effect.
negative_glitch_cols = [
    "flow_duration", "flow_bytes_s", "flow_packets_s",
    "flow_iat_mean", "flow_iat_max", "flow_iat_min", "fwd_iat_min",
    "fwd_header_length34", "bwd_header_length", "fwd_header_length55",
    "min_seg_size_forward",
]
for c in negative_glitch_cols:
    df = df.filter(col(c) >= 0)
after_negative = df.count()

# Remove exact duplicate rows (excluding file/ingestion metadata).
# Drops ~11% of the data, concentrated in scripted attacks (SSH-Patator,
# PortScan) where identical connection attempts genuinely produce identical
# feature vectors. Still the right call — keeping duplicates risks the same
# row landing in both train and test later, letting the model memorize
# instead of generalize.
df = df.dropDuplicates([c for c in df.columns if c not in ("_source_file", "_ingested_at")])
after_dedup = df.count()

# Standardize Web Attack labels. The source file's separator character
# (en-dash) was corrupted into an unrecoverable replacement character
# during CIC's original export — matching on it directly is unreliable.
# Match on the readable prefix + keyword instead, which holds regardless
# of what the corrupted character looks like.
df = df.withColumn(
    "label",
    when(col("label").startswith("Web Attack") & col("label").contains("Brute"), "Web Attack - Brute Force")
    .when(col("label").startswith("Web Attack") & col("label").contains("XSS"), "Web Attack - XSS")
    .when(col("label").startswith("Web Attack") & col("label").contains("Sql"), "Web Attack - SQL Injection")
    .otherwise(trim(col("label")))
)

print(f"Bronze: {before} -> after negative-value filter: {after_negative} -> after dedup: {after_dedup}")
df.write.mode("overwrite").saveAsTable("network_anomaly_detection.silver.flows_clean")