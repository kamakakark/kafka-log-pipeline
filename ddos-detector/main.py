# coding=utf-8
# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
'''
from kafka import KafkaConsumer
from json import loads
from time import sleep



consumer = KafkaConsumer(
    'logging_topic_2',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group-id-6',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

dict_cnt= {}
THRESHOLD_VALUE=3
writeFile = open("foobar2.txt", "a")

for event in consumer:
    event_data = event.value
    # Do whatever you want
    ip = event_data['ip']

    if(ip in dict_cnt.keys()):
       dict_cnt[ip] += 1

    else:
       dict_cnt[ip] = 1

    if(dict_cnt[ip] == THRESHOLD_VALUE):
      writeFile.write(ip + "\n")

      print(ip + "  ----->  " + str(dict_cnt[ip]))

f.close()
'''

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print("Hi, {0}".format(name))  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
