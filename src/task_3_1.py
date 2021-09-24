import pyspark
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import IndexToString
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
from pyspark.sql import SQLContext

spark = SparkSession.builder.appName("Spark").config("master","local[12]").getOrCreate();

firstSchema = StructType([
    StructField("sepal_length", DoubleType(), True), StructField("sepal_width", DoubleType(), True),
    StructField("petal_length", DoubleType(), True), StructField("petal_width", DoubleType(), True),
    StructField("class", StringType(), True)]
)


df_fn = SQLContext(spark.sparkContext).read.format("csv").option("delimiter",",").schema(firstSchema).load("/Users/juancasarescosmen/iris.csv")

col_data = df_fn.columns[:-1]
assembler = pyspark.ml.feature.VectorAssembler(inputCols=col_data, outputCol='features')
df_fn = assembler.transform(df_fn)

df_fn = df_fn.select(['features', 'class'])
label_ind = pyspark.ml.feature.StringIndexer(inputCol='class', outputCol='label').fit(df_fn)
user_lab = label_ind.labels


df_fn = label_ind.transform(df_fn)
df_fn = df_fn.select(['features', 'label'])


train, test = df_fn.randomSplit([0.70, 0.30])

log_reg = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
model = log_reg.fit(train)

pred = model.transform(test)
conv1 = IndexToString(inputCol="label", outputCol="firstlabel")
conv2 = conv1.transform(pred)

conv1 = IndexToString(inputCol="prediction", outputCol="pred_label",labels=user_lab)
conv2 = conv1.transform(conv2)


LastSchema = StructType([
    StructField("sepal_length", DoubleType(), True),StructField("sepal_width", DoubleType(), True),
    StructField("petal_length", DoubleType(), True),StructField("petal_width", DoubleType(), True)])

my_rdd1 = spark.sparkContext.parallelize([[5.1, 3.5, 1.4, 0.2]])
df_fn = sqlContext.createDataFrame(my_rdd1,  LastSchema)
features = assembler.transform(df_fn).select('features')
indexPredicted1 = int(model.predict(features.first()['features']))



my_rdd2 = spark.sparkContext.parallelize([[6.2, 3.4, 5.4, 2.3]])
df_fn = sqlContext.createDataFrame(my_rdd2,  LastSchema)
features = assembler.transform(df_fn).select('features')
indexPredicted2 = int(model.predict(features.first()['features']))


final_31 = open("/Users/juancasarescosmen/out/out_3_1.txt", "w")
final_31.write("Class" + "\n")
final_31.write(user_lab[indexPredicted1])
final_31.write("\n")
final_31.write(user_lab[indexPredicted2])
final_31.close()


