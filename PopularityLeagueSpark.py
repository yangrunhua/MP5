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
league_id_list = leagueIds.collect()

outputFile = open(sys.argv[3], "w", encoding='utf-8')

output = lines.flatMap(mapper).reduceByKey(lambda x, y: x+y).filter(lambda x: x[0] in league_id_list) \
    .sortBy(lambda x: x[1]).collect()

for i in output:
    outputFile.write('%s\t%s\n' % (i[0], i[1]))

last_x = None
final_output = dict()
for x in range(len(output)):
    if last_x:
        final_output[output[x][0]] = x if x[1] != output[last_x][1] else final_output[output[last_x][0]]
    else:
        final_output[output[x][0]] = 0
    last_x = x

for p in sorted(final_output):
    outputFile.write('%s\t%s\n' % (p, final_output[p]))

outputFile.close()
sc.stop()

