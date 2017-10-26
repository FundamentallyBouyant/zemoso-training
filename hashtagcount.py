from __future__ import print_function

import sys,re
from operator import add

from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: hashtagcount <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonHashtagCount")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: re.findall(r'(?i)\#\w+', x)) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add) \
                  .map(lambda a: (a[1], a[0])) \
                  .sortByKey(False)


    for l in counts.take(100): print(l)
    # for (word, count) in output:
    #     print("%s: %i" % (word, count))

spark.stop()