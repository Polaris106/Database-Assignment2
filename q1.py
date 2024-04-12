import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW


# HDFS PATH
df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
)

# removing rows with no reviews or rating < 1.0

def is_empty(col):
  if col:
    return bool(eval(col))
  else:
    return bool(col)


df = df.filter(is_empty(col("Reviews"))).filter(
  col("Rating") >= 1.0 & (col("Rating").isNotNull()))

#To show the csv file, for testing
#df.show()

df.write.csv("hdfs://%s:9000/assignment2/output/question1/" % (hdfs_nn), header=True)