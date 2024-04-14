import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, split, lead, expr
from pyspark.sql.window import Window
# you may add more import if you need to

# don't change this line
# hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW
output_path = '/data/output6'
input_fpath = '/data/tmdb_5000_credits.parquet'

df = spark.read.parquet(input_fpath, header=True)
df_cast = df.select("movie_id", "title", explode(expr(
    "from_json(cast, 'array<struct<cast_id:long,character:string,credit_id:string,gender:long,id:long,name:string,order:long>>')")).alias("cast_info"))
# df_cast.show()
df_cast = df_cast.select("movie_id", "title", col(
    "cast_info.name").alias("actor1"))


# Join the dataframe with itself to find actor pairs co-cast in the same movie
actor_pair = df_cast.alias("a1").join(
    df_cast.alias("a2"),
    (col("a1.movie_id") == col("a2.movie_id")) & (
        col("a1.actor1") != col("a2.actor1"))
).select(
    col("a1.movie_id"),
    col("a1.title"),
    col("a1.actor1").alias("actor1"),
    col("a2.actor1").alias("actor2")
)


# Group by actor pairs and count the number of movies they have worked together
actor_pair_count = actor_pair.groupBy("actor1", "actor2").count()

# Filter to include only actor pairs who have worked in at least 2 movies together
actor_pair_filtered = actor_pair_count.filter(col("count") >= 2)

# Join with original actor_pair dataframe to get movie_id and title
final_result = actor_pair_filtered.join(actor_pair, [
                                        "actor1", "actor2"], "inner").select("movie_id", "title", "actor1", "actor2")

# Show the resulting actor pairs
final_result.show()

# Write result to Parquet files
final_result.write.parquet(output_path, mode="overwrite")
