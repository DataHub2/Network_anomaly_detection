from pyspark.sql.functions import col, desc

df = spark.table("network_anomaly_detection.silver.flows_clean")

print(f"=== Rader kvar: {df.count()} ===")

print("=== Klassfördelning — Web Attack-etiketterna ska nu vara läsbara ===")
df.groupBy("Label").count().orderBy(desc("count")).show(20, truncate=False)

print("=== Kvarstående Infinity (ska vara 0) ===")
for c in ["flow_bytes_s", "flow_packets_s"]:
    n = df.filter(col(c).isin(float("inf"), float("-inf"))).count()
    print(f"{c}: {n}")

print("=== init_win_bytes — ska fortfarande innehålla -1, det är korrekt bevarat ===")
df.select("init_win_bytes_forward", "init_win_bytes_backward").summary("min", "max").show()