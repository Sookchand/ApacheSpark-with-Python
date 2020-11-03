from pyspark import SparkConf, SparkContext

def totalSpent(line):
    fields = line.split(',')
    custID = int(fields[0])
    amntSpent = float(fields[2])
    return(custID, amntSpent)
    
conf = SparkConf().setMaster("local").setAppName("customerOrderSorted")
sc = SparkContext(conf = conf)

data = sc.textFile("file:///sparkcourse/customer-orders.csv")
rdd = data.map(totalSpent)
totalByCutomer = rdd.reduceByKey(lambda x, y: (x + y))
# flipped = totalByCustomer.map(lambda(x, y):(y, x))
# totalSorted = flipped.sortbyKey()
totalSorted = totalByCutomer.map(lambda x: (x[1], x[0])).sortByKey()


results = totalSorted.collect()
for result in results:
    print(result)