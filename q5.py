import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, size, explode, split, from_json

# Don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assignment 2 Question 5").getOrCreate()

# Read movie data from Parquet file
input_path = "hdfs://{}:9000/assignment2/part2/input/tmdb_5000_credits.parquet".format(
    hdfs_nn)
output_path = "hdfs://{}:9000/assignment2/output/question5".format(
    hdfs_nn)

df = spark.read.parquet(input_path)

# Define JSON schema for the cast field
json_schema = "array<struct<cast_id:int, character:string, credit_id:string, gender:int, id:int, name:string, order:int>>"
df = df.withColumn("cast", explode(from_json(df["cast"], json_schema)))

# Explode the cast array to get individual actors/actresses
actor_pairs_df = df.select("id", "title", "cast.name").alias("actor1") \
                   .join(
                       df.select("id", "cast.name").alias("actor2"),
                       col("actor1.id") == col("actor2.id")
) \
    .filter(col("actor1.name") < col("actor2.name")) \
    .select("id", "title", col("actor1.name").alias("actor1"), col("actor2.name").alias("actor2"))

# Group by actor pairs and count the number of movies they co-cast in
co_cast_df = actor_pairs_df.groupBy("actor1", "actor2").agg(
    collect_list("id").alias("movie_ids")
).filter(size("movie_ids") >= 2)

# Explode the list of movie IDs and select required columns
result_df = co_cast_df.select(
    col("movie_ids").getItem(0).alias("movie_id"),
    "title",
    "actor1",
    "actor2"
)

# Save the result into Parquet files with the specified schema
result_df.write.mode("overwrite").parquet(output_path)

spark.stop()
