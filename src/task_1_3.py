from pyspark.sql.functions import desc
from pyspark.shell import sc, sqlContext


rdd = sc.textFile("/Users/juancasarescosmen/Desktop/groceries.csv").flatMap(lambda line: line.split(","))
Count_function = rdd.map(lambda word: (word, 1)).reduceByKey(lambda v1, v2: v1 + v2)
df = sqlContext.createDataFrame(Count_function, ["prod", "count"]).orderBy(desc("count")).limit(30).toPandas()

result = str(df)
print(result)

count = 0
f3 = open("/Users/juancasarescosmen/out/out_1_3.txt", "w")
for line in result.splitlines():
    if count > 0:
        f3.write("('" + (line[3:].lstrip()).replace("    ", "',") + ")" + "\n")
    count += 1