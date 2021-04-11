""" Note:
    To run this program you need to start zookeeper and kafka server else it throws error"""

import unittest
from log-producer import sendLogsToKafka

string = '200.4.91.190 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1.0" 200 3557 "-" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)"'
dict1 = {'remote_ip': '200.4.91.190', 'Date': '25/May/2015', 'time': '23:11:15', 'time_zone': '+0000',
         'request': 'GET / HTTP/1.0', 'status': '200', 'body_bytes_sent': '3557', 'http_referer': '-',
         'http_user_agent': 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)'}

# In string2 making http_user_agent as '-' so value to be '-' for this key
string2 = '209.112.9.34 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1.0" 200 3557 "-" "-"'
dict2 = {'remote_ip': '209.112.9.34', 'Date': '25/May/2015', 'time': '23:11:15', 'time_zone': '+0000',
         'request': 'GET / HTTP/1.0', 'status': '200', 'body_bytes_sent': '3557', 'http_referer': '-',
         'http_user_agent': '-'}

kafka_topic_name = 'test_jsonmsg'

class TestWeblogsParse(unittest.TestCase):

    # Test case to check whether parsing function working or not
    def testFunction_parsed(self):
        global string, string2, dict1, dict2
        self.assertEqual(sendLogsToKafka.parsed(string), dict1)
        self.assertEqual(sendLogsToKafka.parsed(string2), dict2)

    # Test case to check whether return type is dictionary or not
    def test_FunctionReturnType_parsed(self):
        global string, string2
        self.assertEqual(type(sendLogsToKafka.parsed(string)), dict)
        self.assertEqual(type(sendLogsToKafka.parsed(string2)), dict)

    # Test case to check whether python program is sending messages to Kafka or not
    def test_kafkaSendingMessages(self):
        global kafka_topic_name
        for i in range(1,6):
            dict3 = {i: i * 100}
            sendLogsToKafka.kafka_producer_obj.send(kafka_topic_name, dict3)
            print("{} --> Message {} sent Successfully To Kafka".format(dict3,i)) # This will execute only if message sent to kafka


if __name__ == '_main_':
    unittest.main()