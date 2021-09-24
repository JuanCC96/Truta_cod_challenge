from pyspark.shell import sc

rdd = sc.textFile("/Users/juancasarescosmen/Desktop/groceries.csv").map(lambda line: line.split(','))
rddDist = rdd.flatMap(lambda x: x).distinct()


f2a = open("/Users/juancasarescosmen/out/out_1_2a.txt", "w")
for i in rddDistinct.collect():
    f2a.write(str(i) + "\n")
f2a.close()

f2b = open("/Users/juancasarescosmen/out/out_1_2b.txt", "w")
f2b.write("Count:" + "\n")
f2b.write(str(rdd.count()))
f2b.close()