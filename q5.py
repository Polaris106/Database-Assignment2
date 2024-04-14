from itertools import combinations
import pyspark.sql.types as T
import pyspark.sql.functions as F
import sys
from pyspark.sql import SparkSession
# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW

# Q5

data = spark.read.parquet(
    f"hdfs://{hdfs_nn}:9000/assignment2/part2/input/tmdb_5000_credits.parquet", header=True)

# define schema for the "cast" column and new column "parsed_json" containing the parsed JSON data.
schema = T.ArrayType(T.StructType(
    [T.StructField("name", T.StringType(), False)]))
movies2 = data.withColumn("parsejson", F.from_json("cast", schema))

# sort


def pair(x): return combinations(sorted(x), 2)


# schema of the "parsed_json" column from the DataFrame movies2
schema2 = movies2.select("parsejson").schema[0].dataType

get_combinations = F.udf(pair, T.ArrayType(schema2))


# new column named "pairs" by exploding combinations
movies3 = movies2.withColumn("actorpairs", F.explode(
    get_combinations(movies2["parsejson"])))

grouppairs = movies3.groupBy("actorpairs")

countpairs = grouppairs.count()
filtered = countpairs.filter(F.col("count") > 1)
joined = filtered.join(movies3, ["actorpairs"], "left")

# drop unnecessary columns
out = joined.drop("count", "cast", "crew", "parsejson")

# new columns "actor1" and "actor2" by from the pairs, drops the pairs
finalout = out.withColumn("actor1", F.col("actorpairs")[0]["name"])\
    .withColumn("actor2", F.col("actorpairs")[1]["name"])\
    .drop("actorpairs")

finalout.write.csv(
    f"hdfs://{hdfs_nn}:9000/assignment2/output/question5", header=True)
