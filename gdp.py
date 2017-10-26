from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("gdp")\
    .getOrCreate()

df = spark.read.load("../gdp.csv", format="csv",header=True)
df.createOrReplaceTempView("gdp")

sqlDF = spark.sql("""select a.code,a.Year from(
		SELECT t1.`Country Name` as code, t1.Year as Year, t1.Value,((t1.Value - t2.Value) / t2.Value) AS growth
		FROM gdp t1 LEFT JOIN gdp t2 ON t1.`Country Code` = t2.`Country Code` AND t2.Year = t1.Year - 1)
        as a
inner join (
	select Year, max(growth) growth
	from (
		SELECT t1.`Country Code` as code, t1.Year as Year, t1.Value,((t1.Value - t2.Value) / t2.Value) AS growth
		FROM gdp t1 LEFT JOIN gdp t2 ON t1.`Country Code` = t2.`Country Code` AND t2.Year = t1.Year - 1)
        as c
	group by Year
	) b on a.Year = b.Year and a.growth = b.growth order by Year""")
sqlDF.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile("maxgdp.csv")
sqlDF.show()
spark.stop()