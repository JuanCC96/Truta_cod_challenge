from pyspark.shell import sqlContext

df_prop_room = sqlContext.sql("SELECT property_type,"
                    "room_type, "
                    "min(price) as min_price, "
                    "max(price) as_max_price, "
                    "count(price) as row_count "
                    "FROM parquet.`/Users/juancasarescosmen/sf-airbnb-clean.parquet` "
                    "GROUP BY property_type,room_type ")

df_prop_room.toPandas().to_csv('/Users/juancasarescosmen/out/out_2_2.csv', index=False)