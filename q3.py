import sys
from pyspark.sql import SparkSession

# you may add more import if you need to
from pyspark.sql.functions import avg, lit


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW

# load input csv file into Dataframe
input_df = spark.read.csv(
    "hdfs://" + hdfs_nn + "/assignment2/part1/input/TA_restaurants_curated_cleaned.csv", header=True, inferSchema=True)

# group by city and calculate avg rating
city_avg_ratings = input_df.groupBy("City").agg(
    avg("Rating").alias("AvgRating"))

# Find the top 3 cities with the highest average rating
top_cities = city_avg_ratings.orderBy("AvgRating", ascending=False).limit(3)

# Find the top 3 cities with the lowest average rating
bottom_cities = city_avg_ratings.orderBy("AvgRating").limit(3)

# Combine the top and bottom cities into a single DataFrame
combined_cities = top_cities.withColumn("Type", lit("Top")).union(
    bottom_cities.withColumn("Type", lit("Bottom")))

# Write the combined results to a CSV file in HDFS
combined_cities.write.csv(
    "hdfs://" + hdfs_nn + "/assignment2/output/question3/", header=True)

# Stop the SparkSession
spark.stop()
