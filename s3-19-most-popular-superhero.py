from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf = conf)

names = sc.textFile("Marvel-Names.txt")

def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))

namesRdd = names.map(parseNames)

lines = sc.textFile("Marvel-Graph.txt")

def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)

pairings = lines.map(countCoOccurences)
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y)
flipped = totalFriendsByCharacter.map(lambda xy : (xy[1], xy[0]))

mostPopular = flipped.max()

mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print(str(mostPopularName) + " is the most popular superhero, with " + \
    str(mostPopular[0]) + " co-appearances.")
