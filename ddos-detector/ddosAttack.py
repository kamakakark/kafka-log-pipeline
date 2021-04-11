from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, concat_ws, count, window
from pyspark.sql.types import *
# Kafka related ---------------------------------------------------------------------------------------
import kafka

kafka_topic_name = "test-topic-1"
kafka_bootstrap_servers = ["localhost:9092"]
checkpoint_location = "file:///Users/a569514/development/kafka-log-pipeline/log/checkpoint"
hdfs_filepath = "file:///Users/a569514/development/kafka-log-pipeline/log/hdfs"

spark = SparkSession.builder.appName("Find IP of DDOS attacks ").config("spark.streaming.stopGracefullyOnShutdown",
                                                                        True).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", 10)

# defining Schema for weblog data

logSchema = StructType([StructField("remote_ip", StringType(), True), StructField("Date", StringType(), True), \
 \
                        StructField("time", StringType(), True), StructField("time_zone", StringType(), True), \
 \
                        StructField("request", StringType(), True), StructField("status", StringType(), True), \
 \
                        StructField("body_bytes_sent", StringType(), True),
                        StructField("http_referer", StringType(), True), \
 \
                        StructField("http_user_agent", StringType(), True)])

# Reading message from kafka
kafkaStream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap_servers). \
 \
    option("subscribe", kafka_topic_name).option('maxFilesPerTrigger', 1).option("startingOffsets", "earliest").load()

logDF = kafkaStream.withColumn("value", from_json(col("value").cast("string"), logSchema)).selectExpr("value.*"). \
 \
    withColumn("status", col("status").cast("Integer")).withColumn("body_bytes_sent",
                                                                   col("body_bytes_sent").cast("long")). \
 \
    withColumn("log_timestamp", concat_ws(" ", to_date(col("Date"), "dd/MMM/yyyy"), col("time")).cast("timestamp"))

# logDF.writeStream.format("console").start()


# considering watermark to drop late entered data less than 2 minutes


countDF = logDF.withWatermark("log_timestamp", "2 minutes").groupBy(window(col("log_timestamp"), "2 minutes"),
                                                                    col("remote_ip")). \
 \
    agg(count("remote_ip").alias("Num_of_attacks")). \
 \
    select(col("window.start").alias("start_time"), col("remote_ip"), col("Num_of_attacks"))

countDF.writeStream.format("console").outputMode("update").start()

# considering remote_ip who tried to access more than 5 times in 2 minutes are considered as DDOS attacks and writing it into CSV file


ddos_attacks = countDF.filter("Num_of_attacks >=5")

ddos_attacks.writeStream.format("console").outputMode("update").start()


query = ddos_attacks.writeStream.format("csv").outputMode("update").trigger(processingTime="2 minutes"). \
 \
    queryName("DDOS_attacks_tosave").option("checkpointLocation",
                                            checkpoint_location). \
 \
    outputMode("append").start(hdfs_filepath)

query.awaitTermination()
