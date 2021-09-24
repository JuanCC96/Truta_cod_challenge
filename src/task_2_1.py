from pyspark.shell import spark, sqlContext

df = sqlContext.read.parquet("/Users/juancasarescosmen/sf-airbnb-clean.parquet")

df.show()

df.toPandas().to_csv('/Users/juancasarescosmen/out/out_2_1.csv')