from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Spark Juan Truata") \
    .config("master","local[12]") \
    .getOrCreate();

RDD = spark.sparkContext.textFile("/Users/juancasarescosmen/Desktop/groceries.csv")\
    .map(lambda line: line.split(",")) \

print(RDD.take(10))