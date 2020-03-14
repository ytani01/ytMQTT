#!/usr/bin/env python3
#
# (c) 2020 Yoichi Tanibayashi
#
# MqttPublisher
#  simple sample
#
# argv: [topic, token]
#
from Mqtt import BeebottePublisher
import sys

argv = sys.argv

m = BeebottePublisher(argv[2], debug=True)
m.start()

while True:
    s = input('> ')
    m.send_data(s, [argv[1]])
