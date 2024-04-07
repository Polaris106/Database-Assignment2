import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW
# Load the input CSV file into a DataFrame
input_df = spark.read.csv(
    "hdfs://" + hdfs_nn + "/assignment2/part1/input/TA_restaurants_curated_cleaned.csv", header=True, inferSchema=True)

# Count the number of restaurants by city and cuisine style
restaurant_counts = input_df.groupBy("City", "Cuisine Style").agg(
    count("*").alias("Restaurant Count"))

# Write the output to CSV files in HDFS
restaurant_counts.write.csv(
    "hdfs://" + hdfs_nn + "/assignment2/output/question4/restaurant_counts", header=True)

# Stop the SparkSession
spark.stop()
