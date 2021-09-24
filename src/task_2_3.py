from pyspark.shell import sqlContext

df = sqlContext.sql("SELECT  "
                    "avg(bathrooms) as avg_bathrooms, "
                    "avg(bedrooms) as avg_bedrooms"
                    " FROM parquet.`/Users/juancasarescosmen/sf-airbnb-clean.parquet` "
                    "where price > 5000 "
                    "and review_scores_accuracy = 10")

df.toPandas().to_csv('/Users/juancasarescosmen/out/out_2_3.csv', index=False)