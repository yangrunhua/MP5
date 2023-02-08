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

league_count = lines.flatMap(mapper).reduceByKey(lambda x, y: x + y).filter(lambda x: x[0] in league_id_list) \
    .sortBy(lambda x: x[1]).collect()

rank = dict()
for x in range(len(league_count)):
    if x > 0:
        rank[league_count[x][0]] = x if league_count[x][1] != league_count[x - 1][1] else rank[league_count[x - 1][0]]
    else:
        rank[league_count[x][0]] = 0

for p in sorted(rank.keys()):
    outputFile.write('%s\t%s\n' % (p, rank[p]))

outputFile.close()
sc.stop()

