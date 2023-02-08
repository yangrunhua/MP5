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


def custom_split(orig_line):
    c_list = list(orig_line)
    for i in range(len(orig_line)):
        if c_list[i] in delimiters:
            c_list[i] = '*'
    lines_tmp = ''.join(c_list)
    words = lines_tmp.split('*')
    words = [w.strip().lower() for w in words]
    words = [w for w in words if w and not w in stop_words]
    return words


output = lines.flatMap(custom_split).map(lambda w: (w, 1)).reduceByKey(lambda x, y: x + y) \
    .sortByKey().top(10)

outputFile = open(sys.argv[4], "w", encoding='utf-8')

for (word, count) in output:
    outputFile.write("%s\t%i\n" % (word, count))
outputFile.close()
sc.stop()
