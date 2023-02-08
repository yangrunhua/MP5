#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

stop_words = []
delimiters = []

with open(stopWordsPath) as f:
    stop_words = [l.strip().lower() for l in f.readlines()]

with open(delimitersPath) as f:
    line = f.readline()
    delimiters = [l for l in line]

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)

counts = lines.flatMap(lambda l: l.split(" ")).map(lambda w: (w, 1)).reduceByKey(lambda x, y: x + y)

outputFile = open(sys.argv[4], "w", encoding='utf-8')

output = counts.collect()
for (word, count) in output:
    outputFile.write("%s\t%i\n" % (word, count))
outputFile.close()
sc.stop()
