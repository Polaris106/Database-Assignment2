import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, desc, asc, rank
from pyspark.sql.window import Window

# Don't change this line
hdfs_nn = sys.argv[1]

# Initialize SparkSession
spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()

# Load data from HDFS
df = spark.read.option("header", True)\
    .option("inferSchema", True)\
    .option("delimiter", ",")\
    .option("quotes", '"')\
    .csv("hdfs://%s:9000/assignment2/part1/input/" % hdfs_nn)

# Group by city and restaurant, calculate average rating
avg_rating_df = df.groupBy("City", "Name").agg(
    avg("Rating").alias("AverageRating"))

# Create window specifications for ranking
window_desc = Window.partitionBy("City").orderBy(desc("AverageRating"))
window_asc = Window.partitionBy("City").orderBy(asc("AverageRating"))

# Rank cities based on average rating
avg_rating_df = avg_rating_df.withColumn("RankDesc", rank().over(window_desc)) \
    .withColumn("RankAsc", rank().over(window_asc))

# Filter top 3 and bottom 3 cities
top_3_cities = avg_rating_df.filter(
    "RankDesc <= 3").select("City", "AverageRating")
bottom_3_cities = avg_rating_df.filter(
    "RankAsc <= 3").select("City", "AverageRating")

# Add RatingGroup column
top_3_cities = top_3_cities.withColumn("RatingGroup", "High")
bottom_3_cities = bottom_3_cities.withColumn("RatingGroup", "Low")

# Combine top and bottom cities
result_df = top_3_cities.union(bottom_3_cities)

# Write the output as CSV files into the specified HDFS path
result_df.write.csv(
    "hdfs://%s:9000/assignment2/output/question3/" % hdfs_nn, header=True)

# Write the output as CSV files into the specified HDFS path
result_df.write.csv(
    "hdfs://%s:9000/assignment2/output/question3/" % hdfs_nn, header=True)
