# kafka-pysaprk-log-pipeline

STREAMING APPLICATION FOR DETECTING DDoS ATTACKS 

PROJECT:  To find DDoS attacks for a given webserver and block the IP’s to prevent the web traffic using Big data Technologies.
 
DDoS attack in simple words?
A distributed denial-of-service (DDoS) attack is a malicious attempt to disrupt the normal traffic of a targeted server, 
service or network by overwhelming the target or its surrounding infrastructure with a flood of Internet traffic.

Brief: Executive team would like to know the DDoS attacks happening frequently on their website for every 2 minutes and 
block them to clear the traffic overhead for their website.
Environment: HDFS (for storage), Python (for converting raw text data into json), apache Kafka (for messaging), Spark
 Structured Streaming (for streaming analytics using Python Data-Frame API’s)
Steps to Solve the problem:
Using python program transformed text data into json format and send messages continuously to apache Kafka (Distributed messaging system) 
using Kafka producer.
Using Spark Structured Streaming, subscribed to Kafka topics and extracted json subfields from them and further processed 
them as useful data and converted them to streaming Data-Frame.
Counting the IP for every 2 minutes window and filtering the IP addresses if count goes above 25, system will mark it 
as DDoS attack and block this IP address.
Subsequently storing each above data-frame into HDFS in CSV format.

