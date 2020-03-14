#!/usr/bin/env python3
#
# (c) 2020 Yoichi Tanibayashi
#
# MqttSubscriber
#  simple sample
#
# argv: [topic, token]
#
from Mqtt import BeebotteSubscriber
import sys

argv = sys.argv

def cb_func(data, topic, ts):
    print(data, topic, ts)

s = BeebotteSubscriber(cb_func, argv[1], argv[2], debug=True)
s.start()

while True:
    pass
