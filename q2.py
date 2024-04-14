import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as max_, min as min_, row_number
from pyspark.sql.window import Window
# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assignment 2 Question 2").getOrCreate()

input_file_name = 'hdfs://%s:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv' % (
    hdfs_nn)
output_dir_name = 'hdfs://%s:9000/assignment2/output/question2' % (hdfs_nn)

df = spark.read.csv(input_file_name, header=True)
df = df.filter(df["Price Range"].isNotNull())

df = df.withColumn("Rating", df["Rating"].cast("double"))

grouped_df = df.groupBy("City", "Price Range").agg(
    max_("Rating").alias("Max Rating"), min_("Rating").alias("Min Rating"))

# row numbers partitioned by City and Price Range, ordered by Rating
window_best = Window.partitionBy(
    "City", "Price Range").orderBy(col("Rating").desc())
window_worst = Window.partitionBy(
    "City", "Price Range").orderBy(col("Rating").asc())

# top-ranked and lowest-ranked restaurants bc there are multiple 5 star and 1 star for each city/price range
best_restaurants = df.withColumn("rank", row_number().over(
    window_best)).filter(col("rank") == 1).drop("rank")
worst_restaurants = df.withColumn("rank", row_number().over(
    window_worst)).filter(col("rank") == 1).drop("rank")

# best + worst
result_df = best_restaurants.union(worst_restaurants).orderBy("City")
result_df.show()
# result_df.show()
result_df.write.mode('overwrite').option("header", True).csv(output_dir_name)
spark.stop()
