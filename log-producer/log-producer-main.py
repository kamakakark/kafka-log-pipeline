
from json import dumps
from kafka import KafkaProducer
import sys
import os

TOPIC_NAME='logging_topic_3'

def consume_logs(bootstrap_server, input_file):

    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_server],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )
    print input_file
    if os.path.isfile(input_file):
        current_file = open(input_file, "r")
        for line in current_file:
            if (line != None):
                producer.send(TOPIC_NAME, value=line)
    else:
        print "provided path does not exists : "+input_file

def main(arguments):

    #print 'Arguments %s' %arguments
    print '-----------------------------------------------------------------------------'
    print 'Function Name    :  main(arguments)'
    print 'bootstrap-server  :  %s' %arguments[1]
    print 'Input-file  :  %s' %arguments[2]
    consume_logs(arguments[1], arguments[2])

if __name__ == '__main__':
    if len(sys.argv) == 3:
        main(sys.argv[0:])

    else:
        print('Argument syntax error.\nProgram syntax: python log-producer-main.py <bootstrap_server> <input_file_path> '
              '\n example python log-producer-main.py  localhost:9092 filpath.txt')
        sys.exit(1)


