import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, size, explode
from itertools import combinations

# Don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assignment 2 Question 5").getOrCreate()

# Read movie data from Parquet file
input_path = "hdfs://{}:9000/assignment2/part2/input/tmdb_5000_credits.parquet".format(
    hdfs_nn)
output_path = "hdfs://{}:9000/assignment2/part2/input/tmdb_5000_credits.parquet".format(
    hdfs_nn)

df = spark.read.option("header", "true").parquet(input_path)

# Extract pairs of actors/actresses for each movie
actor_pairs_df = df.select("movie_id", "title", explode("cast").alias("actor")).alias("actor").join(
    df.select("movie_id", explode("cast").alias("actor")).alias("other_actor"),
    (col("actor.movie_id") == col("other_actor.movie_id")) & (
        col("actor.actor") < col("other_actor.actor"))
).select(
    "movie_id",
    "title",
    col("actor.actor").alias("actor1"),
    col("other_actor.actor").alias("actor2")
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
result_df.write.csv(output_path)
