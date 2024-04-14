import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, size
from itertools import combinations

# Don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assignment 2 Question 5").getOrCreate()

# Read movie data from Parquet file
movies_df = spark.read.parquet(hdfs_nn + "/path/to/movies")

# Extract pairs of actors/actresses for each movie
actor_pairs_df = movies_df.select("movie_id", "title", "cast").explode("cast").alias("actor").join(
    movies_df.select("movie_id", "cast").explode("cast").alias("other_actor"),
    col("actor.cast") < col("other_actor.cast")
).select(
    "movie_id",
    "title",
    col("actor.cast").alias("actor1"),
    col("other_actor.cast").alias("actor2")
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
result_df.write.mode("overwrite").parquet(hdfs_nn + "/path/to/output")
