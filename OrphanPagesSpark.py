#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)

def mapper(line):
    src, dest = line.strip().split(':', 1)
    dest_list = [i.strip() for i in dest.split(' ') if i]
    result = [(src.strip(), 0)]
    for d in dest_list:
        if d and not d == src.strip():
            result.append((d, 1))
    return result

outputFile = open(sys.argv[2], "w", encoding='utf-8')

output = lines.flatMap(mapper)
outputFile.writelines(output.collect()[0] + str(output.collect()[1]))
outputFile.write("\n==========\n")
output = output.reduceByKey(lambda x, y: x+y)
outputFile.writelines(type(output.collect()))
outputFile.write("\n==========\n")
output = output.filter(lambda x: x == 0)
outputFile.writelines(type(output.collect()))
outputFile.write("\n==========\n")
output = output.collect()


for i in sorted(output, key=lambda x: str(x[0])):
    outputFile.write(i + "\n")

outputFile.close()
sc.stop()

