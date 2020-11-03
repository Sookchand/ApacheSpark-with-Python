from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("friendsByAge").getOrCreate()

line = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///SparkCourse/fakefriends-header.csv")
    

# selecting the friends and age column
friendsByAge = line.select("age", "friends")

# now I am going to group by age "age" and compute avg
friendsByAge.groupBy("age").avg("friends").show()

# sorting
friendsByAge.groupBy("age").avg("friends").sort("age").show()

#further formating
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

# with a custom column name
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("friends_avg")).sort("age").show()

spark.stop()

