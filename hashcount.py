import re
from operator import add

from pyspark import SparkConf, SparkContext
conf = (SparkConf() \
       .setMaster("local[*]") \
       .setAppName("hashtagcount"))

sc = SparkContext(conf = conf)

lines = sc.textFile('../spark-training/tweets-1.txt')
counts = lines.flatMap(lambda x: re.findall(r'#(\w+)', x)) \
              .map(lambda x: (x, 1)) \
              .reduceByKey(add) \
              .map(lambda a: (a[1], a[0])) \
              .sortByKey(False)

counts.coalesce(1).saveAsTextFile("hashtagcount.csv")

for l in counts.take(100): print(l)
