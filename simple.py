#!/usr/bin/env python3
#
# (C) 2019 Yoichi Tanibayashi
#
__author__ = 'Yoichi Tanibayashi'
__date__   = '2019'

import paho.mqtt.client as mqtt
import json
import time
from MyLogger import get_logger
import click
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


class Foo:
    DEF_HOST = 'localhost'
    DEF_PORT = 1883
    DEF_QOS = 0

    def __init__(self, user, topics, mqtt_host=DEF_HOST, mqtt_port=DEF_PORT,
                 debug=False):
        self._dbg = debug
        self._log = get_logger(__class__.__name__, self._dbg)
        self._log.debug('user=%s, topics=%s', user, topics)
        self._log.debug('mqtt_host=%s, mqtt_port=%s', mqtt_host, mqtt_port)

        self._user = user
        self._topics = topics
        self._mqtt_host = mqtt_host
        self._mqtt_port = mqtt_port

        self._mqttc = mqtt.Client()
        self._mqttc.on_connect = self.on_connect
        self._mqttc.on_disconnect = self.on_disconnect
        self._mqttc.on_message = self.on_message
        self._mqttc.on_publish = self.on_publish
        self._mqttc.username_pw_set(self._user, '')

    def main(self):
        self._log.debug('')

        self._log.debug('connect')
        self._mqttc.connect(self._mqtt_host, self._mqtt_port, keepalive=10)
        time.sleep(2)
        
        self._log.debug('loop_start')
        self._mqttc.loop_start()
        time.sleep(2)
        
        while True:
            indata = input('> ')
            if indata == '':
                break

            for t in self._topics:
                payload = json.dumps(indata).encode('utf-8')
                self._log.debug('publish: payload=%a', payload)
                self._mqttc.publish(t, payload, qos=self.DEF_QOS, retain=False)
                time.sleep(2)

    def end(self):
        self._log.debug('')

        self._log.debug('loop_stop')
        self._mqttc.loop_stop()
        time.sleep(2)

        self._log.debug('disconnect')
        self._mqttc.disconnect()
        time.sleep(2)

        self._log.debug('done')

    def on_connect(self, client, userdata, flag, rc):
        self._log.debug('userdata=%s, flag=%s, rc=%s', userdata, flag, rc)

        time.sleep(2)

        for t in self._topics:
            self._log.debug('subscribe')
            self._mqttc.subscribe(t, qos=self.DEF_QOS)
            time.sleep(2)

        self._log.debug('done')

    def on_disconnect(self, client, userdata, rc):
        self._log.debug('userdata=%s, rc=%s', userdata, rc)
        time.sleep(1)
        self._log.debug('done')

    def on_message(self, client, userdata, msg):
        self._log.debug('userdata=%s', userdata)

        topic = msg.topic
        payload = json.loads(msg.payload.decode('utf-8'))
        self._log.debug('done:topic=%s, payload=%a', topic, payload)

    def on_publish(self, client, userdata, mid):
        self._log.debug('done:userdata=%s, mid=%s', userdata, mid)


@click.command(context_settings=CONTEXT_SETTINGS,
               help='''
''')
@click.argument('user')
@click.argument('mqtt_host')
@click.argument('topic1')
@click.argument('topic2', nargs=-1)
@click.option('--mqtt_port', '--port', '-p', 'mqtt_port', type=int,
              default=Foo.DEF_PORT,
              help='server port')
@click.option('--debug', '-d', 'debug', is_flag=True, default=False,
              help='debug flag')
def main(user, mqtt_host, mqtt_port, topic1, topic2, debug):
    log = get_logger(__name__, debug=debug)

    topics = [topic1] + list(topic2)

    app = Foo(user, topics, mqtt_host, mqtt_port, debug)
    try:
        app.main()
    finally:
        log.debug('finally')
        app.end()

    log.debug('done')


if __name__ == '__main__':
    main()
