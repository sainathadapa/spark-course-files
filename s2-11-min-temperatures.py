from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

lines = sc.textFile("1800.csv")

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    # converting to fahrenheit
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

parsedLines = lines.map(parseLine)

minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])

stationTemps = minTemps.map(lambda x: (x[0], x[2]))

minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))

results = minTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
