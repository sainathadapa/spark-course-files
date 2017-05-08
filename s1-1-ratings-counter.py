# importing what we need
from pyspark import SparkConf, SparkContext
import collections

# set up our context
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

# load the data
lines = sc.textFile("ml-100k/u.data")
# extract (map) the data we care about
ratings = lines.map(lambda x: x.split()[2])
# perform an action: count by value
result = ratings.countByValue()

# sort and display results
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
