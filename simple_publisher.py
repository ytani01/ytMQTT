#!/usr/bin/env python3
#
# (c) 2020 Yoichi Tanibayashi
#
# MqttPublisher
#  simple sample
#
# argv: [topic, user, password, host]
#
from MqttClientServer import MqttPublisher
import sys

argv = sys.argv

m = MqttPublisher(argv[1], argv[2], argv[3], argv[4])
m.start()

while True:
    s = input('> ')
    m.send_data(s)
