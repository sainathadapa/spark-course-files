from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('TallyUp')
sc = SparkContext(conf = conf)

lines = sc.textFile('customer-orders.csv')

def id_spent_fn(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

id_spent = lines.map(id_spent_fn)

id_spent_totals = id_spent.reduceByKey(lambda x,y: x+y)

totals_sorted = id_spent_totals.map(lambda x: (x[1], x[0])).sortByKey()

results = totals_sorted.collect()

for count, id in results:
    print (id, count)


