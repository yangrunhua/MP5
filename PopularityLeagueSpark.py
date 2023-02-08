#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
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

leagueIds = sc.textFile(sys.argv[2], 1)
league_id_list = leagueIds.flatMap(lambda line: line.strip()).collect()
output = output.filter(lambda x: x[0] in league_id_list).map(lambda x: (x[0], x[1], 0)).sortBy(lambda x: x[1]).collect()

outputFile = open(sys.argv[3], "w", encoding='utf-8')
last_x = None
for x in output:
    if last_x:
        x[2] = last_x[2] + 1 if x[1] != last_x[1] else last_x[2]
    last_x = x

output.sort(key=lambda x: x[0])
for p in output:
    outputFile.write('%s\t%s' % (p[0], p[2]))

#TODO
#write results to output file. Foramt for each line: (key + \t + value +"\n")

sc.stop()

