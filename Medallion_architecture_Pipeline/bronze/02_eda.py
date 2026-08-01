from pyspark.sql.functions import col, sum as spark_sum, desc

df = spark.table("network_anomaly_detection.bronze.flows")

print("=== Class sortation ===")
df.groupBy("Label").count().orderBy(desc("count")).show(20)

numeric_cols = [c for c, t in df.dtypes if t in ("double", "int", "bigint")]

print("=== Missing values värden ===")
df.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in numeric_cols]).show()

print("=== Infinity-värden ===")
for c in numeric_cols:
    n = df.filter(col(c).isin(float("inf"), float("-inf"))).count()
    if n > 0:
        print(f"{c}: {n}")

print("=== Rows per file ===")
df.groupBy("source_file").count().show(truncate=False)