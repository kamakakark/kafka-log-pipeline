from kafka.errors import KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, concat_ws, count, window
from pyspark.sql.types import *
import sys

kafka_topic_name = "test-topic-3"
kafka_bootstrap_servers = "localhost:9092"
checkpoint_location = "file:///Users/a569514/development/kafka-log-pipeline/output-files/checkpoint"
hdfs_filepath = "file:///Users/a569514/development/kafka-log-pipeline/output-files/hdfs"

# defining Schema for weblog data
logSchema = StructType([StructField("remote_ip", StringType(), True), StructField("Date", StringType(), True),
                        StructField("time", StringType(), True), StructField("time_zone", StringType(), True),
                        StructField("request", StringType(), True), StructField("status", StringType(), True),
                        StructField("body_bytes_sent", StringType(), True),
                        StructField("http_referer", StringType(), True),
                        StructField("http_user_agent", StringType(), True)])

def get_spark_session():
    try:
        spark = SparkSession.builder.appName("Find IP of DDOS attacks ").config("spark.streaming.stopGracefullyOnShutdown",                                                                            True).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        spark.conf.set("spark.sql.shuffle.partitions", 10)
        return spark
    except Exception as ex:
        print("Exception during consuming topic - {}".format(ex))

def get_kafka_stream():
    try:
        spark_session = get_spark_session()
        return spark_session.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap_servers). \
            option("subscribe", kafka_topic_name).option("startingOffsets", "earliest").load()
    except KafkaError as ex:
        print("Exception during consuming topic - {}".format(ex))

def ddos_detecctor(ip_threshold, time_window):

    try:
        proccess_time = time_window.strip + " minutes"
        # Reading message from kafka
        kafkaStream =  get_kafka_stream()

        logDF = kafkaStream.withColumn("value", from_json(col("value").cast("string"), logSchema)).selectExpr("value.remote_ip", "value.Date", "value.time"). \
            withColumn("log_timestamp", concat_ws(" ", to_date(col("Date"), "dd/MMM/yyyy"), col("time")).cast("timestamp"))
        #withColumn("status", col("status").cast("Integer")).withColumn("body_bytes_sent",col("body_bytes_sent").cast("long")).\
        # logDF.writeStream.format("console").start()
        # considering watermark to drop late entered data less than 2 minutes


        countDF = logDF.withWatermark("log_timestamp", proccess_time).groupBy(window(col("log_timestamp"), proccess_time),
            col("remote_ip")). \
            agg(count("remote_ip").alias("Num_of_attacks")). \
            select(col("window.start").alias("start_time"), col("remote_ip"), col("Num_of_attacks"))

        countDF.writeStream.format("console").outputMode("update").start()
        # considering remote_ip who tried to access more than 5 times in 2 minutes are considered as DDOS attacks and writing it into CSV file

        ddos_attacks = countDF.filter(col("Num_of_attacks") >= int(ip_threshold))
        ddos_attacks.writeStream.format("console").trigger(processingTime=proccess_time).outputMode("complete").start()

        query = ddos_attacks.writeStream.format("csv").outputMode("append").trigger(processingTime=proccess_time). \
            queryName("DDOS_attacks_tosave").option("checkpointLocation",
                                                    checkpoint_location). \
            start(hdfs_filepath)
        query.awaitTermination()
    except Exception as ex:
        print("Exception during processing DDOS Detection - {}".format(ex))

def main(arguments):
    print("IP threshold - {}".format(arguments[1]))
    print("Time window - {}".format(arguments[2]))
    ddos_detecctor(arguments[1], arguments[2])

if __name__ == '__main__':
    if len(sys.argv) == 3:
        main(sys.argv[0:])

    else:
        print('Argument syntax error.\nProgram syntax: spark-submit ddosAttack.py <ip_threshold> <timewindow>'
              '\n example spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2 ddosAttack.py "localhost:9092" "kafka-topic" "5" ')
        sys.exit(1)