from pyspark.shell import sqlContext

df = sqlContext.sql("SELECT  neighbourhood_cleansed, "
                    "max(accommodates) as max_accommodates,"
                    "min(price) as min_price, "
                    "max(review_scores_rating) as max_ratings "
                    "FROM parquet.`/Users/juancasarescosmen/sf-airbnb-clean.parquet` "
                    "group by neighbourhood_cleansed ")

df.toPandas().to_csv('/Users/juancasarescosmen/out/out_2_4.csv', index=False)