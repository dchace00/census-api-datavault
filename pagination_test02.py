from pyspark.sql import SparkSession
import time

# Create a Spark session
spark = SparkSession.builder \
    .appName("dbt-pyspark-batch-size-test") \
    .config("spark.jars", "/path/to/postgresql-42.2.20.jar") \
    .getOrCreate()

# Read data from PostgreSQL database in chunks
def load_data_in_chunks(chunk_size):
    offset = 0
    while True:
        start_time = time.time()

        query = f"""
        SELECT DISTINCT d.year, g.geo_key, v.var_key
        FROM stage._data_ d
        JOIN stage_refined.hub_geo g ON d.geoid = g.geoid AND d.geoname = g.geoname
        JOIN stage_refined.hub_var v ON d.var_id = v.var_id AND d.product = v.product
        LIMIT {chunk_size} OFFSET {offset};
        """

        # Load chunk of data into a Spark DataFrame
        df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/census_dbt_sandbox") \
            .option("dbtable", f"({query}) as subquery") \
            .option("user", "postgres") \
            .option("password", "Blubberboil") \
            .option("driver", "org.postgresql.Driver") \
            .load()

        if df.rdd.isEmpty():
            break

        # Calculate and print processing time
        processing_time = time.time() - start_time
        print(f"Chunk size: {chunk_size}, Offset: {offset}, Time: {processing_time:.2f}s")

        # Update the offset for pagination
        offset += chunk_size

# Test different chunk sizes
for size in [1000, 5000, 10000, 25000]:
    load_data_in_chunks(size)

spark.stop()
