# coding=utf-8
from time import sleep
from json import dumps
from kafka import KafkaProducer
import re


def consume_logs():
    f = open("/Users/a569514/Downloads/others/foobar.txt", "r")
    # f = open("foobar.txt", "r")

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    for x in f:
        out = re.findall('([^ ]) - - \[(.)\] "(.)" ([0-9]) ([0-9]) "(.)" "(.*)"', x)
        ip, timestamp = out[0][0:2]
        if ip == None or len(ip) < 5:
            print("===========> EMPTY IP")
        if timestamp == None or len(timestamp) < 4:
            print("===========> EMPTY timestamp")
        logMsg = {}
        # print(ip + " --- " +timestamp)
        logMsg['ip'] = ip
        logMsg['timestamp'] = timestamp
        producer.send('logging_topic_2', value=logMsg)
        # sleep(0.5)


if __name__ == '__main__':
    consume_logs()

