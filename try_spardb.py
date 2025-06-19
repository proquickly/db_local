from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("SaveDataFrameToDelta")
    .config(
        "spark.jars.packages",
        "io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)

print("Spark version:", spark.version)
print("Spark configurations:")
for conf in spark.sparkContext.getConf().getAll():
    if "delta" in conf[0].lower() or "catalog" in conf[0].lower():
        print(f"  {conf[0]}: {conf[1]}")

data = [{"id": 1, "name": "Alice", "age": 30}]
df = spark.createDataFrame(data)
df.show()

df.write.format("delta").mode("overwrite").save(
    "/Users/andy/ws/projects/db_local/delta-table"
)
print("Success!")
