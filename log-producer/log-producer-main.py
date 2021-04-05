
import os
import sys
from json import dumps

from kafka import KafkaProducer
from kafka.errors import KafkaError

TOPIC_NAME='logging_topic_4'

def get_kafka_producer(bootstrap_server):
    try:
        return KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )
    except KafkaError as ex:
        print("Exception during kafka producer connection - {}".format(ex))

def consume_logs(bootstrap_server, input_file):

    producer = get_kafka_producer(bootstrap_server)
    if os.path.isfile(input_file):
        current_file = open(input_file, "r")
        for line in current_file:
            if (line != None):
                producer.send(TOPIC_NAME, value=line)
    else:
        print("Provided path does not exists : {}".format(input_file))

def main(arguments):
    print("bootstrap-server  - {}".format(arguments[1]))
    print("Input-file - {}".format(arguments[2]))
    consume_logs(arguments[1], arguments[2])

if __name__ == '__main__':
    if len(sys.argv) == 3:
        main(sys.argv[0:])

    else:
        print('Argument syntax error.\nProgram syntax: python log-producer-main.py <bootstrap_server> <input_file_path> '
              '\n example python log-producer-main.py  localhost:9092 filpath.txt')
        sys.exit(1)


