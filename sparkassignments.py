import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import split, col, count, hour, minute, second

spark=SparkSession.builder.appName("sparkbyexamples").getOrCreate()
header_schems = StructType([
    StructField("Log", StringType(), True),
    StructField("date-time-stamp", StringType(), True),
    StructField("user_id", StringType(), True),StructField("torrent", StringType(), True)])

csvFiles = spark.read.option("header",True).schema(header_schems).csv("Resources/ghtorrent-logs.txt")


df1 = csvFiles.withColumn('torrent',split(col("user_id"),"--").getItem(0)) \
            .withColumn('getapiclient',split(col("user_id"),"--").getItem(1)) \
            .withColumn('clientresult',split(col("getapiclient"),":").getItem(0)) \
            .withColumn('req_client',split(col("getapiclient"),":").getItem(1)) \
            .withColumn('request', split(col("req_client"), ".").getItem(0))

df1.show(truncate=False)

count_line = df1.agg(count("*").alias("total_count"))
count_line.sort("total_count").show()

wranlog = df1.filter("Log = 'WARN'").agg(count("*").alias("totalwran_count"))
wranlog.sort("totalwran_count").show()

rep_api = df1.filter(col("clientresult").like("%api_client.rb%")).agg(count("*").alias("api_claim"))
rep_api.sort("api_claim").show()

http_req = df1.groupBy("torrent").agg(count("*").alias("http_req_count"))
http_req.sort("http_req_count").show()

Failed_req = df1.filter(col("req_client").like("%Failed%")).groupBy("torrent").agg(count("*").alias("Failed_request"))
Failed_req.sort("Failed_request").show()

active_hour = df1.withColumn("hour", hour(col("date-time-stamp"))).groupBy("hour").agg(count("*").alias("active_hours"))
active_hour.sort("active_hours").show(truncate=False)

active_repository = df1.groupBy("torrent").agg(count("*").alias("active_repository"))
active_repository.sort("active_repository").show(truncate=False)