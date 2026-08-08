from pyspark.sql.functions import col, desc

df = spark.table("network_anomaly_detection.silver.flows_clean")

print(f" Rows left: {df.count()} ")

print(" Klassfördelning — Web Attack-etiketterna should now be readable")
df.groupBy("Label").count().orderBy(desc("count")).show(20, truncate=False)

print(" remaining Infinity (has to be 0) ")
for c in ["flow_bytes_s", "flow_packets_s"]:
    n = df.filter(col(c).isin(float("inf"), float("-inf"))).count()
    print(f"{c}: {n}")

print(" init_win_bytes — should still have -1, and that should be correct ")
df.select("init_win_bytes_forward", "init_win_bytes_backward").summary("min", "max").show()