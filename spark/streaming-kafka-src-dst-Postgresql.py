from os import truncate
from pyspark.sql import SparkSession

from pyspark.sql.types import MapType,StringType
from pyspark.sql.functions import from_json

spark=(SparkSession \
    .builder \
    .master('local') \
    .appName('kafka-postgre-streaming') \
    .config("spark.jars","/home/emre/spark-3.0.1-bin-hadoop2.7/jars/postgresql-42.3.1.jar") \
    .getOrCreate()
)



df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe", "applications") \
    .load()

df1 = df.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")

df1.createOrReplaceTempView("applies")

res = spark.sql("SELECT * FROM applies") \

print(res)

def foreach_batch_function(df, epoch_id):
  
    mode = "overwrite"
    url = "jdbc:postgresql://localhost:5432/yemeksepetidb"
    properties = {"user": "postgres","password": "Pa55w.rd","driver": "org.postgresql.Driver"}
    table="restaurant_url_new"
    df2=df.withColumn("value",from_json(df.value,MapType(StringType(),StringType()))) 
    df3= df2.select(["value.company_name","value.company_url","value.application_date"])
    df3.write.jdbc(url=url, table=table,mode=mode, properties=properties)
   
    pass

df1.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()