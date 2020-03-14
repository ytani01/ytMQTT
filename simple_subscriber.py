#!/usr/bin/env python3
#
# (c) 2020 Yoichi Tanibayashi
#
# MqttSubscriber
#  simple sample
#
# argv: [topic, user, password, host]
#
from Mqtt import MqttSubscriber
import sys

argv = sys.argv

def cb_func(data, ts):
    print(data, ts)

s = MqttSubscriber(cb_func, argv[1], argv[2], argv[3], argv[4])
s.start()

while True:
    pass
