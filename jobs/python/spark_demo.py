from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("DEMO").getOrCreate()

data = [("1","Omkar","Revankar"),("2","SONI","Revankar")]
schema = ["Id","FirstName","LastName"]
df = spark.createDataFrame(data,schema).withColumn("FullName",F.concat(F.col("FirstName"),F.lit(" "),F.col("LastName")))
df.show()

spark.stop()