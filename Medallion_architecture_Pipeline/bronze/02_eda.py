from pyspark.sql.functions import col, sum as spark_sum, desc

# 1. Load the raw Bronze table
df = spark.table("network_anomaly_detection.bronze.flows")

# 2. Check target class distribution (attack types vs benign traffic)
print("=== Class Distribution ===")
df.groupBy("Label").count().orderBy(desc("count")).show(20)

# Identify all numeric columns for statistical checks
numeric_cols = [c for c, t in df.dtypes if t in ("double", "int", "bigint")]

# 3. Count missing (null) values across all numeric columns
print("=== Missing Values ===")
df.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in numeric_cols]).show()

# 4. Check for positive and negative Infinity values (common artifact in CIC-IDS2017)
print("=== Infinity Values ===")
for c in numeric_cols:
    n = df.filter(col(c).isin(float("inf"), float("-inf"))).count()
    if n > 0:
        print(f"{c}: {n}")

# 5. Verify row count contribution per ingested source file
print("=== Rows Per File ===")
df.groupBy("_source_file").count().show(truncate=False)

# 6. Identify columns containing negative values (glitches vs sentinel values)
print("=== Negative Values ===")
for c in numeric_cols:
    n = df.filter(col(c) < 0).count()
    if n > 0:
        print(f"{c}: {n}")