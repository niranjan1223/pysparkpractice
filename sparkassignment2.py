from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import findspark

spark = SparkSession.builder.appName('ass1').getOrCreate()
#reading data from csv
df = spark.read.option("header",True).option("inferSchema",True).csv("resources/assigment1.csv")
#1.converting issue date to timestamp format
df1 = df.withColumn("equal_time", to_timestamp(col("Issue_Date")/1000)).show(truncate=False)
#2.converting timestamp to date format
df2 = df.withColumn("equal_time",from_unixtime(col("Issue_Date") / 1000)).withColumn("epoch_milli",concat(unix_timestamp("equal_time"), F.date_format("equal_time", "S")))
#3.removing the extra space
df.withColumn("Country",when(col("Country")=="null" ,"").otherwise(col("Country"))).show()
#4.removing null values in country column
df.withColumn("col2",ltrim("Brand")).show()  
