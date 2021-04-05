
import os
import sys
from datetime import datetime
from json import loads

from kafka import KafkaConsumer
from kafka.errors import KafkaError

dict_cnt= {}
THRESHOLD_VALUE=50
KAFKA_GROUP_ID='my-group-id-8'
TOPIC_NAME='logging_topic_4'

def kafka_consumer(bootstrap_server):

    try:
        return KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[bootstrap_server],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=KAFKA_GROUP_ID,
            value_deserializer=lambda x: loads(x.decode('utf-8'))
        )

    except KafkaError as ex:
        print("Exception during consuming topic - {}".format(ex))

def ddos_detecctor(bootstrap_server, output_file):

    if os.path.isfile(output_file):
        consumer = kafka_consumer(bootstrap_server)
        writeFile = open(output_file, "a")
        datetime_for_header= str(datetime.now())
        writeFile.write(datetime_for_header + "\n")

        for event in consumer:
            if(event != None):
                event_data = event.value
                ip=event_data.split(" ")[0]
                if(ip in dict_cnt.keys()):
                    dict_cnt[ip] += 1
                    if(dict_cnt[ip] == THRESHOLD_VALUE):
                        print("DDOS Attack Detected for ip - {}".format(ip))
                        writeFile.write(ip + "\n")
                else:
                    dict_cnt[ip] = 1
        writeFile.close()

    else:
        print("Provided path does not exists : {}".format(output_file))

def main(arguments):
    print("bootstrap-server  - {}".format(arguments[1]))
    print("Output-file - {}".format(arguments[2]))
    ddos_detecctor(arguments[1], arguments[2])

if __name__ == '__main__':
    if len(sys.argv) == 3:
        main(sys.argv[0:])

    else:
        print('Argument syntax error.\nProgram syntax: python main.py <bootstrap_server> <input_file_path> '
              '\n example python main.py  localhost:9092 filpath.txt')
        sys.exit(1)
