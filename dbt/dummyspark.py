from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .appName("local dbt")
        .master("local[*]")  # run locally using all cores
        .config("spark.driver.bindAddress", "localhost")
        .config("spark.driver.host", "localhost")
        # Extra configs to stabilize
        .config("spark.executor.heartbeatInterval", "30s")
        .config("spark.network.timeout", "120s")
        # JARS: Delta Lake + Postgres driver
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.postgresql:postgresql:42.6.0")
        # Enable Delta
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
)

#spark.sql("SELECT 1").show()

#spark.sql("DROP TABLE IF EXISTS default_gold.gold_attrition_prototype")
#spark.sql("DROP TABLE IF EXISTS default_gold.gold_attrition_department_prototype")

# List tables in schema
#spark.sql("SHOW TABLES IN default_gold").show()

# Query the view
df = spark.sql("SELECT * FROM default_gold.gold_attrition_prototype LIMIT 5")
df.write.format("delta").mode("overwrite").save("C:/UpScale/HRProject/HRDataPipeline/analytics/delta/gold/gold_attrition_prototype")

#print(spark.sql("SHOW DATABASES").show(truncate=False))

#df.show()

print("done")