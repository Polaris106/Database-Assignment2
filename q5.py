import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, size, explode, split
from itertools import combinations

# Don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assignment 2 Question 5").getOrCreate()

# Read movie data from Parquet file
input_path = "hdfs://{}:9000/assignment2/part2/input/tmdb_5000_credits.parquet".format(
    hdfs_nn)
output_path = "hdfs://{}:9000/assignment2/output/question5".format(
    hdfs_nn)

df = spark.read.parquet(input_path)

# Extract actor/actress pairs for each movie
actor_pairs_df = df.select(
    col("movie_id"),
    col("title"),
    explode(split(col("cast"), ",\s*")).alias("actor1")
).join(
    df.select(
        col("id").alias("movie_id"),
        explode(split(col("cast"), ",\s*")).alias("actor2")
    ),
    "movie_id"
).filter(
    col("actor1") < col("actor2")
).select(
    "movie_id",
    "title",
    col("actor1").alias("actor1"),
    col("actor2").alias("actor2")
)

# Group by actor pairs and count the number of movies they co-cast in
co_cast_df = actor_pairs_df.groupBy("actor1", "actor2").agg(
    collect_list("movie_id").alias("movie_ids")
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
