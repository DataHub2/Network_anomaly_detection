from pyspark.sql.functions import col, when

bronze = spark.table("network_anomaly_detection.bronze.flows")

negative_glitch_cols = [
    "flow_duration", "flow_bytes_s", "flow_packets_s",
    "flow_iat_mean", "flow_iat_max", "flow_iat_min", "fwd_iat_min",
    "fwd_header_length34", "bwd_header_length", "fwd_header_length55",
    "min_seg_size_forward",
]

df = bronze
for c in ["flow_bytes_s", "flow_packets_s"]:
    df = df.withColumn(c, when(col(c).isin(float("inf"), float("-inf")), 0.0).otherwise(col(c)))
for c in negative_glitch_cols:
    df = df.filter(col(c) >= 0)

before = df.groupBy("Label").count().withColumnRenamed("count", "before_dedup")

deduped = df.dropDuplicates([c for c in df.columns if c not in ("_source_file", "_ingested_at")])
after = deduped.groupBy("Label").count().withColumnRenamed("count", "after_dedup")

comparison = (
    before.join(after, "Label", "outer")
    .withColumn("dropped", col("before_dedup") - col("after_dedup"))
    .withColumn("pct_dropped", (col("dropped") / col("before_dedup") * 100))
)
comparison.orderBy(col("pct_dropped").desc()).show(20, truncate=False)