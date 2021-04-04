# coding=utf-8
# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
'''
from time import sleep
from json import dumps
from kafka import KafkaProducer
import re

f = open("/Users/a569514/Downloads/others/foobar.txt", "r")
#f = open("foobar.txt", "r")

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

for x in f:
  out = re.findall('([^ ]) - - \[(.)\] "(.)" ([0-9]) ([0-9]) "(.)" "(.*)"', x)
  ip, timestamp = out[0][0:2]
  if(ip==None or len(ip) < 5):
    print("===========> EMPTY IP")
  if(timestamp==None or len(timestamp) < 4):
    print("===========> EMPTY timestamp")
  logMsg={}
  #print(ip + " --- " +timestamp)
  logMsg['ip'] = ip
  logMsg['timestamp'] = timestamp
  producer.send('logging_topic_2', value=logMsg)
  #sleep(0.5)

'''




def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print("Hi, {0}".format(name))  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
