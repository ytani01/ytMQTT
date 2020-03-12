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
import random
import threading

from MyLogger import get_logger


class Mqtt:
    CONF_FILENAME = ['mqtt.conf', '.mqtt.conf']
    CONF_PATH = ['.', os.environ['HOME'], '/etc']

    DEF_HOST = 'localhost'
    DEF_PORT = 1883

    DEF_QOS = 0

    MSG_OK     = 'OK'      # {'type':MSG_OK,     'data':'message'}
    MSG_CON    = 'CON'     # {'type':MSG_CON,    'data':rc}
    MSG_DISCON = 'DISCON'  # {'type':MSG_DISCON, 'data':rc}
    MSG_DATA   = 'DATA'    # {'type':MSG_DATA, 'data':{'topic':t, 'payload':p}}
    MSG_NONE   = 'NONE'    # {'type':MSG_NONE,   'data':None}
    MSG_ERR    = 'ERR'     # {'type':MSG_ERR,    'data':'mesage'}

    def __init__(self, topic, user, host=DEF_HOST, port=DEF_PORT,
                 debug=False):
        self._debug = debug
        self._logger = get_logger(__class__.__name__, self._debug)
        self._logger.debug('topic=%s', topic)
        self._logger.debug('user=%s, host=%s, port=%d', user, host, port)

        if type(topic) == str:
            topic = [topic]

        if len(topic) == 0:
            return

        self._topic = topic
        self._user = user
        self._svr_host = host
        self._svr_port = port

        self._conf = []
        self._pw   = ''

        self._msgq = queue.Queue()

        if self.load_conf() is None:
            raise RuntimeError('load_conf(): failed')

        self._pw = self.get_pw(self._user)
        if self._pw is None:
            raise RuntimeError('get_pw(): failed (ret=%s)' % self._pw)

        self._logger.debug('_user=\'%s\', _pw=\'%s\'', self._user, self._pw)

        self._mqttc = mqtt.Client()
        # self._mqttc.enable_logger()
        # self._mqttc.on_log = self.on_log
        self._mqttc.on_connect = self.on_connect
        self._mqttc.on_disconnect = self.on_disconnect
        self._mqttc.on_message = self.on_message
        self._mqttc.on_publish = self.on_publish

        self._mqttc.username_pw_set(self._user, self._pw)

        self._loop_active = False

    def end(self):
        self._logger.debug('')

        self.disconnect()
        self.put_msg(self.MSG_DISCON, 0)

        msg_type, msg_data = self.wait_msg(self.MSG_DISCON)
        self._logger.debug('%s: %s', msg_type, msg_data)
        self.loop_stop()

        self._logger.debug('done')

    def send_data(self, topic, data):
        self._logger.debug('topic=%s, data=%a', topic, data)

        self.publish(topic, data)

    def recv_data(self, topic):
        self._logger.debug('topic=%s', topic)

        while self._loop_active:
            try:
                msg_type, msg_data = self.wait_msg(self.MSG_DATA)
                self._logger.debug('msg_type=%s, msg_data=%s',
                                   msg_type, msg_data)

                if msg_data['topic'] == topic:
                    return msg_data['payload']

            except Exception as e:
                return None

            self.put_msg(msg_type, msg_data)
            time.sleep(random.random())

        self._logger.info('done')
        return None

    def put_msg(self, msg_type, msg_data):
        self._logger.debug('msg_type=%s, msg_data=%s', msg_type, msg_data)

        msg = {'type': msg_type, 'data': msg_data}
        self._msgq.put(msg)

    def get_msg(self, block=False, timeout=None):
        self._logger.debug('block=%s, timeout=%s', block, timeout)

        try:
            msg = self._msgq.get(block=block, timeout=timeout)
        except queue.Empty:
            msg = {'type': self.MSG_NONE, 'data': None}

        self._logger.debug('msg=%s', msg)
        return msg['type'], msg['data']

    def wait_msg(self, wait_msg_type):
        self._logger.debug('wait_msg_type=%s', wait_msg_type)
        self._logger.debug('_loop_active=%s', self._loop_active)

        (msg_type, msg_data) = (self.MSG_NONE, None)

        while self._loop_active:
            msg_type, msg_data = self.get_msg(block=True, timeout=2)
            self._logger.debug('wait_msgtype=%s, msg_type=%s, msg_data=%s',
                              wait_msg_type, msg_type, msg_data)

            if msg_type == wait_msg_type:
                return msg_type, msg_data

            if msg_type == self.MSG_NONE:
                continue

            self.put_msg(msg_type, msg_data)
            time.sleep(random.random())

        self._logger.info('done')
        return None

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
        self._loop_active = True

    def loop_stop(self):
        self._logger.debug('')
        self._loop_active = False
        self._mqttc.loop_stop()

    def publish(self, topic, payload, qos=DEF_QOS, retain=False):
        self._logger.debug('topic=%s, payload=%s, qos=%d, retain=%s',
                           topic, payload, qos, retain)

        if topic is None or topic == '':
            topic = self._topic[0]
            self._logger.debug('topic=%s', topic)

        if type(topic) == int:
            topic = self._topic[topic]
            self._logger.debug('topic=%s', topic)

        msg_payload = json.dumps(payload).encode('utf-8')
        self._logger.debug('msg_payload=%s', msg_payload)

        self._mqttc.publish(topic, msg_payload, qos=qos, retain=retain)

        self.wait_msg(self.MSG_OK)

    def subscribe(self, topic=None, qos=DEF_QOS):
        self._logger.debug('topic=%s, qos=%d', topic, qos)

        if topic is not None:
            if type(topic) == int:
                topic = self._topic[topic]
                self._logger.debug('topic=%s', topic)
            self._mqttc.subscribe(topic, qos=qos)
            return

        # topic == None
        for topic in self._topic:
            self._logger.debug('topic=%s', topic)
            self._mqttc.subscribe(topic, qos=qos)

    def unsubscribe(self, topic=None, ):
        self._logger.debug('topic=%s', topic)

        if topic is not None:
            self._mqttc.unsibscribe(topic)
            return

        for t in self._topic:
            self._mqttc.unsubscribe(t)

    def on_log(self, client, userdata, level, buf):
        self._logger.debug('userdata=%s, level=%d, buf=%s',
                           userdata, level, buf)

    def on_connect(self, client, userdata, flag, rc):
        self._logger.debug('userdata=%s, flag=%s, rc=%s', userdata, flag, rc)

        if rc != 0:
            self.put_msg(self.MSG_ERR, 'connect error')
            return

        self._logger.debug('put_msg(MSG_CON)')
        self.put_msg(self.MSG_CON, rc)

    def on_disconnect(self, client, userdata, rc):
        self._logger.debug('userdata=%s, rc=%s', userdata, rc)

        if rc != 0:
            self.put_msg(self.MSG_ERR, 'disconnect error')
            return

        self.put_msg(self.MSG_DISCON, rc)

    def on_message(self, client, userdata, msg):
        self._logger.debug('userdata=%s', userdata)

        topic = msg.topic

        self._logger.debug('msg.payload=%s', msg.payload)
        payload = json.loads(msg.payload.decode('utf-8'))
        self._logger.debug('topic=%s, payload=%s', topic, payload)

        msg_data = {'topic': topic, 'payload': payload}
        self.put_msg(self.MSG_DATA, msg_data)

    def on_publish(self, client, userdata, mid):
        self._logger.debug('userdata=%s, mid=%s', userdata, mid)
        self.put_msg(self.MSG_OK, 'published(mid=%s)' % mid)

    def load_conf(self):
        self._logger.debug('')

        conf_file = self.find_conf()
        self._logger.debug('conf_file=%s', conf_file)
        if conf_file is None:
            return None

        with open(conf_file, 'r') as f:
            csv_reader = csv.reader(f, skipinitialspace=True, quotechar='"')
            for row in csv_reader:
                # self._logger.debug('row=%s', row)
                if row[0].startswith('#'):
                    continue
                while len(row) < 5:
                    row.append('')
                conf_ent = {'host': row[0], 'port': int(row[1]),
                            'topic': row[2],
                            'user': row[3], 'pw': row[4]}
                # self._logger.debug('conf_ent=%s', conf_ent)
                self._conf.append(conf_ent)

        # self._logger.debug('_conf=%s', self._conf)
        return self._conf

    def find_conf(self):
        self._logger.debug('')

        for dir in self.CONF_PATH:
            for fname in self.CONF_FILENAME:
                pathname = dir + '/' + fname
                self._logger.debug('pathname=%s', pathname)
                if os.path.isfile(pathname) or os.path.islink(pathname):
                    return pathname
        return None

    def get_pw(self, user):
        """
        topicが複数の場合は、先頭のトピックがマッチする
        """
        self._logger.debug('user=%s', user)
        self._logger.debug('host=%s, port=%s, topic=%s',
                           self._svr_host, self._svr_port, self._topic[0])

        for cf in self._conf:
            # self._logger.debug('cf=%s', cf)
            if cf['host'] == self._svr_host and \
               cf['port'] == self._svr_port and \
               cf['topic'] == self._topic[0] and \
               cf['user'] == user:
                pw = cf['pw']
                self._logger.debug('user=%s, pw=%s', user, pw)
                return pw

        return None

    def start(self):
        self._logger.debug('')

        self.connect()
        self.loop_start()

        msg_type, msg_data = self.wait_msg(self.MSG_CON)
        self._logger.debug('%s: %s', msg_type, msg_data)

        self._logger.debug('done')


class MqttApp:
    def __init__(self, mqtt, debug=False):
        self._debug = debug
        self._logger = get_logger(__class__.__name__, self._debug)
        self._logger.info('mqtt=%s', type(mqtt))

        self._mqtt = mqtt
        self._topic = self._mqtt._topic[0]
        self._active = False

        self._th = threading.Thread(target=self.receiver)

    def main(self):
        self._logger.info('')

        self._mqtt.start()
        self._mqtt.subscribe()

        self._active = True
        self._th.start()

        while self._active:
            data = input('--==< OK >==--\n')

            if data == '':
                self._active = False
                break
            try:
                data = float(data)
                if int(data) == data:
                    data = int(data)
            except ValueError:
                pass
            self._logger.debug('data=%a', data)

            self._mqtt.send_data(self._topic, data)

        self._logger.info('done')

    def end(self):
        self._logger.info('')
        self._active = False

        self._logger.info('_mqtt.end() ..')
        self._mqtt.end()

        self._logger.info('_th.join() ..')
        self._th.join()

        self._logger.info('done')

    def receiver(self):
        self._logger.debug('')

        while self._active:
            data = self._mqtt.recv_data(self._topic)
            self._logger.debug('data=%s', data)
            if data is None:
                continue

            print('> %a' % (data))

        self._logger.info('done')


class MqttServerApp:
    def __init__(self, mqtt, debug=False):
        self._debug = debug
        self._logger = get_logger(__class__.__name__, self._debug)
        self._logger.debug('mqtt=%s', type(mqtt))

        self._mqtt = mqtt

        self._topic = self._mqtt._topic
        if len(self._topic) < 2:
            raise RuntimeError('len(_topic) < 2: %s' % self._topic)

        self._topic_request = self._topic[0]
        self._topic_reply = self._topic[1]

    def main(self):
        self._logger.debug('')

        self._mqtt.start()
        self._mqtt.subscribe(self._topic_request)

        self._active = True

        while self._active:
            data = self._mqtt.recv_data(self._topic_request)
            self._logger.info('recv[%s]: data="%s"', self._topic_request, data)

            th = threading.Thread(target=self.handle, args=([data]))
            th.start()

        self._logger.debug('done')

    def end(self):
        self._logger.info('')
        self._active = False
        self._logger.info('done')

    def handle(self, data):
        time.sleep(1)

        self._mqtt.send_data(self._topic_reply, data)
        self._logger.info('send[%s]: data="%s"', self._topic_reply, data)


class MqttClientApp:
    def __init__(self, mqtt, debug=False):
        self._debug = debug
        self._logger = get_logger(__class__.__name__, self._debug)
        self._logger.debug('mqtt=%s', type(mqtt))

        self._mqtt = mqtt

        self._topic = self._mqtt._topic
        if len(self._topic) < 2:
            raise RuntimeError('len(_topic) <2: %s' % self._topic)

        self._topic_request = self._topic[0]
        self._topic_reply = self._topic[1]

        self._mqtt.start()
        self._mqtt.subscribe(self._topic_reply)

        self.th = threading.Thread(target=self.receiver)

        self._active = True

    def main(self):
        self._logger.debug('')
        self.th.start()

        while self._active:
            data = input('--==< OK >==--\n')
            self._logger.debug('data="%s"', data)
            if data == '':
                self._active = False
                break

            self._mqtt.send_data(self._topic_request, data)

        self._logger.debug('done')

    def receiver(self):
        self._logger.debug('')

        while self._active:
            data = self._mqtt.recv_data(self._topic_reply)
            print('"%s"' % (data))

        self._logger.debug('done')

    def end(self):
        self._logger.debug('')
        self._active = False

        self._mqtt.end()

        self.th.join()
        self._logger.debug('done')


import click
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS,
               help='''
MQTT Common class
''')
@click.argument('user')
@click.argument('mqtt_host')
@click.argument('topic1')
@click.argument('topic2', nargs=-1)
@click.option('--mqtt_port', '--port', '-p', 'mqtt_port', type=int,
              default=Mqtt.DEF_PORT,
              help='server port')
@click.option('--mode', '-m', 'mode', type=str, default='',
              help='mode: \'\' or \'s\' or \'c\'')
@click.option('--debug', '-d', 'debug', is_flag=True, default=False,
              help='debug flag')
def main(user, mqtt_host, mqtt_port, topic1, topic2, mode, debug):
    logger = get_logger(__name__, debug=debug)

    topic = [topic1] + list(topic2)

    mqtt = Mqtt(topic, user, mqtt_host, mqtt_port, debug=debug)
    app = None

    if mode == '':
        app = MqttApp(mqtt, debug=debug)

    if mode != '':
        if len(topic) != 2:
            print('topics must be .. {request topic} {reply topic}')
            return

    if mode == 'c':
        app = MqttClientApp(mqtt, debug=debug)

    if mode == 's':
        if topic[0] == topic[1]:
            print('topics must be .. {request topic} {reply topic}')
            return
        app = MqttServerApp(mqtt, debug=debug)

    if app is None:
        return

    try:
        app.main()
    finally:
        logger.info('finally')
        app.end()


if __name__ == '__main__':
    main()
