#!/usr/bin/env python3
#
# (C) 2019 Yoichi Tanibayashi
#
__author__ = 'Yoichi Tanibayashi'
__date__   = '2019'

import paho.mqtt.client as mqtt
import queue
import os
import csv
import json
import time

from MyLogger import get_logger


class Mqtt:
    CONF_FILENAME = ['mqtt.conf', '.mqtt.conf']
    CONF_PATH = ['.', os.environ['HOME'], '/etc']

    DEF_PORT = 1883

    DEF_QOS = 0

    MSG_OK   = 0    # {'type': MSG_OK,   'data': 'message'}
    MSG_DATA = 1    # {'type': MSG_DATA, 'data': {data}}
    MSG_ERR  = 255  # {'type': MSG_ERR,  'data': 'mesage'}

    def __init__(self, host, topic, port=DEF_PORT, debug=False):
        self._debug = debug
        self._logger = get_logger(__class__.__name__, self._debug)
        self._logger.debug('host=%s:port=%d, topic=%s', host, port, topic)

        if len(topic) == 0:
            return

        self._svr_host = host
        self._svr_port = port
        self._topic = topic

        self._conf = []

        self._user = ''
        self._pw   = ''

        self._msgq = queue.Queue()

        if self.load_conf() is None:
            self._logger.error('load_conf(): failed')
            return

        ret = self.get_user_pw()
        if ret is None:
            self._logger.error('get_user_pw(): failed (ret=%s)', ret)
            return

        self._user, self._pw = ret
        self._logger.debug('_user=\'%s\', _pw=\'%s\'', self._user, self._pw)

        self._mqttc = mqtt.Client()
        self._mqttc.enable_logger()
        self._mqttc.on_log = self.on_log
        self._mqttc.on_connect = self.on_connect
        self._mqttc.on_disconnect = self.on_disconnect
        self._mqttc.on_message = self.on_message
        self._mqttc.on_publish = self.on_publish

        self._mqttc.username_pw_set(self._user, self._pw)

    def msg_put(self, msg_type, msg_data):
        self._logger.debug('msg_type=%s, msg_data=%s', msg_type, msg_data)

        msg = {'type': msg_type, 'data': msg_data}
        self._msgq.put(msg)

    def msg_get(self):
        self._logger.debug('')

        msg = self._msgq.get()
        self._logger.debug('msg=%s', msg)
        return msg['type'], msg['data']

    def connect(self, keepalive=60):
        self._logger.debug('')
        self._mqttc.connect(self._svr_host, self._svr_port,
                            keepalive=keepalive)

    def disconnect(self):
        self._logger.debug('')
        self._mqttc.disconnect()

    def loop_start(self):
        self._logger.debug('')
        self._mqttc.loop_start()

    def loop_stop(self):
        self._logger.debug('')
        self._mqttc.loop_stop()

    def subscribe(self, topic=None, qos=DEF_QOS):
        self._logger.debug('topic=%s, qos=%d', topic, qos)

        if topic is not None:
            self._mqttc.subscribe(topic, qos=qos)
            return

        for t in self._topic:
            self._mqttc.subscribe(t, qos=qos)

    def unsubscribe(self, topic=None, ):
        self._logger.debug('topic=%s', topic)

        if topic is not None:
            self._mqttc.unsibscribe(topic)
            return

        for t in self._topic:
            self._mqttc.unsubscribe(t)

    def publish(self, topic, payload, qos=DEF_QOS, retain=False):
        self._logger.debug('topic=%s, payload=%s, qos=%d, retain=%s',
                           topic, payload, qos, retain)

        msg_payload = json.dumps(payload).encode('utf-8')
        self._logger.debug('msg_payload=%s', msg_payload)
        self._mqttc.publish(topic, msg_payload, qos=qos, retain=retain)

    def on_log(self, client, userdata, level, buf):
        self._logger.debug('userdata=%s, level=%d, buf=%s',
                           userdata, level, buf)

    def on_connect(self, client, userdata, flag, rc):
        self._logger.debug('userdata=%s, flag=%s, rc=%s', userdata, flag, rc)

        if rc != 0:
            self.msg_put(self.MSG_ERR, 'connect error')
            return

        self.msg_put(self.MSG_OK, 'connect ok')

    def on_disconnect(self, client, userdata, rc):
        self._logger.debug('userdata=%s, rc=%s', userdata, rc)

        if rc != 0:
            self.msg_put(self.MSG_ERR, 'disconnect error')
            return

        self.msg_put(self.MSG_OK, 'disconnect ok')

    def on_message(self, client, userdata, msg):
        self._logger.debug('userdata=%s', userdata)

        topic = msg.topic

        self._logger.debug('msg.payload=%s', msg.payload)
        payload = json.loads(msg.payload.decode('utf-8'))
        self._logger.debug('topic=%s, payload=%s', topic, payload)

        msg_data = {'topic': topic, 'data': payload}
        self.msg_put(self.MSG_DATA, msg_data)

    def on_publish(self, client, userdata, mid):
        self._logger.debug('userdata=%s, mid=%s', userdata, mid)
        self.msg_put(self.MSG_OK, 'published(mid=%s)' % mid)

    def find_conf(self):
        self._logger.debug('')

        for dir in self.CONF_PATH:
            for fname in self.CONF_FILENAME:
                pathname = dir + '/' + fname
                self._logger.debug('pathname=%s', pathname)
                if os.path.isfile(pathname) or os.path.islink(pathname):
                    return pathname
        return None

    def load_conf(self):
        self._logger.debug('')

        conf_file = self.find_conf()
        self._logger.debug('conf_file=%s', conf_file)
        if conf_file is None:
            return None

        with open(conf_file, 'r') as f:
            csv_reader = csv.reader(f, skipinitialspace=True, quotechar='"')
            for row in csv_reader:
                self._logger.debug('row=%s', row)
                if row[0].startswith('#'):
                    continue
                while len(row) < 5:
                    row.append('')
                conf_ent = {'host': row[0], 'port': int(row[1]),
                            'topic': row[2],
                            'user': row[3], 'pw': row[4]}
                self._logger.debug('conf_ent=%s', conf_ent)
                self._conf.append(conf_ent)

        self._logger.debug('_conf=%s', self._conf)
        return self._conf

    def get_user_pw(self):
        """
        topicが複数の場合は、先頭のトピックで検索する
        """
        self._logger.debug('')

        for cf in self._conf:
            self._logger.debug('cf=%s', cf)
            if cf['host'] == self._svr_host and \
               cf['port'] == self._svr_port and \
               cf['topic'] == self._topic[0]:
                user = cf['user']
                pw = cf['pw']
                self._logger.debug('user=%s, pw=%s', user, pw)
                return user, pw

        return None

    def start(self):
        self._logger.debug('')

        self.connect()
        self.loop_start()
        time.sleep(1)

        """
        msg_type, msg_data = self.msg_get()
        self._logger.debug('%s: %s', msg_type, msg_data)
        """

    def end(self):
        self._logger.debug('')

        self.disconnect()
        msg_type, msg_data = self.msg_get()
        self._logger.debug('%s: %s', msg_type, msg_data)

        self.loop_stop()


class App:
    def __init__(self, host, topic, port=Mqtt.DEF_PORT, debug=False):
        self._debug = debug
        self._logger = get_logger(__class__.__name__, self._debug)
        self._logger.debug('host=%s:port=%d, topic=%s', host, port, topic)

        self._svr_host = host
        self._svr_port = port
        self._topic = topic

        self._mqtt = Mqtt(self._svr_host, self._topic, self._svr_port,
                          debug=self._debug)
        self._mqtt.start()

    def main(self):
        self._logger.debug('')

        self._mqtt.subscribe()
        self._mqtt.publish('env1/temp', 'hello')

        for i in range(5):
            msg_type, msg_data = self._mqtt.msg_get()
            if msg_type == Mqtt.MSG_DATA:
                topic = msg_data['topic']
                data = msg_data['data']
                print('(%d) %s: %s' % (i, topic, data))
            else:
                print('(%d) %s' % (i, msg_data))
            time.sleep(2)

    def end(self):
        self._logger.debug('')
        self._mqtt.end()


import click
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS,
               help='''
MQTT Common class
''')
@click.argument('server_host')
@click.argument('topic1')
@click.argument('topic2', nargs=-1)
@click.option('--server_port', '--port', '-p', 'server_port', type=int,
              default=Mqtt.DEF_PORT,
              help='server port')
@click.option('--debug', '-d', 'debug', is_flag=True, default=False,
              help='debug flag')
def main(server_host, server_port, topic1, topic2, debug):
    logger = get_logger(__name__, debug=debug)

    topic = [topic1] + list(topic2)

    app = App(server_host, topic, server_port, debug=debug)
    try:
        app.main()
    finally:
        logger.info('finally')
        app.end()


if __name__ == '__main__':
    main()
