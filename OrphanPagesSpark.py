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


output = lines.flatMap(mapper).reduceByKey(lambda x, y: x+y).filter(lambda x: x == 0).collect()

outputFile = open(sys.argv[2], "w", encoding='utf-8')


for i in sorted(output, key=lambda x: str(x[0])):
    outputFile.write(i + "\n")

outputFile.close()
sc.stop()

