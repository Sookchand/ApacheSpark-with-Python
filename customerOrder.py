import re
from pyspark import SparkConf, SparkContext

def totalSpent(line):
    fields = line.split(',')
    custID = int(fields[0])
    amntSpent = float(fields[2])
    return(custID, amntSpent)
    
conf = SparkConf().setMaster("local").setAppName("customerOrder")
sc = SparkContext(conf = conf)

data = sc.textFile("file:///sparkcourse/customer-orders.csv")
rdd = data.map(totalSpent)
totalByCutomer = rdd.reduceByKey(lambda x, y: (x + y))

results = totalByCutomer.collect()
for result in results:
    print(result {:.2f})



