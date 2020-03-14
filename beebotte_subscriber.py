#!/usr/bin/env python3
#
# (c) 2020 Yoichi Tanibayashi
#
# MqttSubscriber
#  simple sample
#
# argv: [topic, token]
#
from MqttClientServer import BeebotteSubscriber
import sys

argv = sys.argv

def cb_func(data, ts):
    print(data, ts)

#s = BeebotteSubscriber(cb_func, argv[1], argv[2], debug=False)
s = BeebotteSubscriber(cb_func, ['env1/temp', 'env1/humidity'], argv[2], debug=False)
s.start()

while True:
    pass
