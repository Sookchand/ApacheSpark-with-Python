from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

co = SparkSession.builder.appName("TotalSpentByCustomer").master("local[*]").getOrCreate()

schema = StructType([ \
                     StructField("cus_ID", IntegerType(), True),
                     StructField("item_ID", IntegerType(), True),
                     StructField("amntSpent", FloatType(), True)
                     ])

df = co.read.schema(schema).csv("file:///SparkCourse/customer-orders.csv")


custAvgSpent = df.groupBy("cus_ID").agg(func.round(func.sum("amntSpent"), 2) \
                                      .alias("total_spent"))

totalSorted = custAvgSpent.sort("total_spent")

totalSorted.show(totalSorted.count())

co.stop()