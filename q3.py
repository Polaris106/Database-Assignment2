import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

# Don't change this line
hdfs_nn = sys.argv[1]

# Initialize SparkSession
spark = SparkSession.builder.appName("Assignment 2 Question 3").getOrCreate()

# Load data from HDFS
df = spark.read.option("header", True)\
    .option("inferSchema", True)\
    .option("delimiter", ",")\
    .option("quotes", '"')\
    .csv("hdfs://%s:9000/assignment2/part1/input/" % hdfs_nn)

# Calculate the average rating per city
avg_rating_df = df.groupBy("City").agg(
    avg("Rating").alias("AverageRating"))

# Determine the three cities with the highest average rating
top_cities_df = avg_rating_df.orderBy(col("AverageRating").desc()).limit(
    3).withColumn("RatingGroup", "Top")

# Determine the three cities with the lowest average rating
bottom_cities_df = avg_rating_df.orderBy(
    col("AverageRating")).limit(3).withColumn("RatingGroup", "Bottom")

# Combine the top and bottom cities dataframes
combined_df = top_cities_df.union(bottom_cities_df)

# Sort the combined DataFrame
sorted_df = combined_df.orderBy(
    col("RatingGroup").desc(), col("AverageRating").desc())

# Selecting only necessary columns
result_df = sorted_df.select("City", "AverageRating", "RatingGroup")

# Write the output as CSV files into the specified HDFS path
result_df.write.csv(
    "hdfs://%s:9000/assignment2/output/question3/" % hdfs_nn, header=True)
