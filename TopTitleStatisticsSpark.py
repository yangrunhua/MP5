#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)

output = lines.flatMap(lambda line: [int(line.split('\t', 1)[1])])
outputFile = open(sys.argv[2], "w", encoding='utf-8')

outputFile.write('Mean\t%s\n' % int(output.mean()))
outputFile.write('Sum\t%s\n' % int(output.sum()))
outputFile.write('Min\t%s\n' % int(output.min()))
outputFile.write('Max\t%s\n' % int(output.max()))
outputFile.write('Var\t%s\n' % int(output.variance()))

outputFile.close()
sc.stop()

