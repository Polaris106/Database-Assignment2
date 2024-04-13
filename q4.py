import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Don't change this line
hdfs_nn = sys.argv[1]

# Initialize SparkSession
spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()

# Load data from HDFS
df = spark.read.option("header", True)\
    .option("inferSchema", True)\
    .option("delimiter", ",")\
    .option("quotes", '"')\
    .csv("hdfs://%s:9000/assignment2/part1/input/" % hdfs_nn)

# Group by city and cuisine style, count the number of restaurants
result_df = df.groupBy("City", "Cuisine Style")\
              .agg(count("*").alias("Count"))

# Write the output as CSV files into the specified HDFS path
result_df.write.csv(
    "hdfs://%s:9000/assignment2/output/question4/" % hdfs_nn, header=True)
