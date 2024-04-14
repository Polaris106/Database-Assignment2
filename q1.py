import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()

# HDFS PATH
df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
)

# Filter rows with no reviews or rating < 1.0
new_df = df.filter((col("Reviews").isNotNull()) & (col("Rating") >= 1.0))

# Write the cleaned dataframe to HDFS as CSV
new_df.write.csv(
    "hdfs://%s:9000/assignment2/output/question1/" % (hdfs_nn), header=True)
