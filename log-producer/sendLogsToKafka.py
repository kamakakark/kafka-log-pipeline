import logging
import os
import shutil
import time
import json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
# Kafka related ---------------------------------------------------------------------------------------
from kafka import KafkaProducer
from kafka.errors import KafkaError

sourceFolder = '/Users/a569514/development/kafka-log-pipeline/files/source'
destinationFolder = '/Users/a569514/development/kafka-log-pipeline/files/destination'
kafka_topic_name = "test-topic-4"
kafka_bootstrap_servers = ["localhost:9092"]

kafka_producer_obj = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers, value_serializer=lambda v: json.dumps(v). \
                                   encode('utf-8'))


# Defining function to parse weblogs

def parsed(line):
    keyValue = {}
    try:
        fields = line.split(" ", 5)
        keyValue["remote_ip"] = fields[0].strip()
        keyValue["Date"] = fields[3].strip().split(":", 1)[0].replace("[", "")
        keyValue["time"] = fields[3].strip().split(":", 1)[1]
        keyValue["time_zone"] = fields[4].strip().replace("]", "")
        keyValue["request"] = fields[5].strip().split("\"", 2)[1]
        keyValue["status"] = fields[5].strip().split("\"", 3)[2].strip(" ").split(" ")[0]
        keyValue["body_bytes_sent"] = fields[5].strip().split("\"", 3)[2].strip(" ").split(" ")[1]
        keyValue["http_referer"] = fields[5].strip().split("\"")[3]
        keyValue["http_user_agent"] = fields[5].strip().split("\"")[5]
    except Exception as e:
        # logging.exception(e)
        print(e)
    return keyValue


# File monitoring using watchdog
class MyEventHandler(FileSystemEventHandler):
    def on_any_event(self, event):
        logging.info("File {} was just {}".format(event.src_path, event.event_type))
        try:
            if event.event_type == 'created':
                with open(event.src_path, errors='replace') as log:
                    message = log.readlines()
                    for line in message:
                        temp = parsed(line.strip())
                        if temp != {}:
                            kafka_producer_obj.send(kafka_topic_name, temp)
                        time.sleep(0.1)
                print("Successfully file {} has been sent to kafka".format(event.src_path))
                shutil.move(event.src_path, destinationFolder)
            elif event.event_type == 'moved' or event.event_type == 'deleted':
                logging.info("File {} was just {}".format(event.src_path, event.event_type))
                time.sleep(0.5)
        except KafkaError as k:
            logging.exception(k)
            # print(k)
            pass
        except Exception as e:
            logging.exception(e)
            # print(e)


def main(folder_name=None):
    logging.basicConfig(filename='webserverError.log', filemode='a', level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    watched_dir = os.path.dirname(folder_name)
    print('watched_dir = {watched_dir}'.format(watched_dir=watched_dir))
    event_handler = MyEventHandler()
    observer = Observer()
    observer.schedule(event_handler, watched_dir, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == '__main__':
    main(sourceFolder)
