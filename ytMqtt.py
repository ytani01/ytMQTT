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
    MSG_CON    = 'CON'     # {'type':MSG_CON,    'data':{'rc':rc,'flag':flag}
    MSG_DISCON = 'DISCON'  # {'type':MSG_DISCON, 'data':{'rc':rc}
    MSG_SUB    = 'SUB'     # {'type':MSG_SUB,    'data':{'mid':mid,'qos':q}
    MSG_UNSUB  = 'UNSUB'   # {'type':MSG_UNSUB,  'data':{'rc':rc}
    MSG_PUB    = 'PUB'     # {'type':MSG_PUB,    'data':{'mid':mid}
    MSG_DATA   = 'DATA'    # {'type':MSG_DATA,  'data':{'topic':t,'payload':p}}
    MSG_NONE   = 'NONE'    # {'type':MSG_NONE,   'data':None}
    MSG_ERR    = 'ERR'     # {'type':MSG_ERR,    'data':'mesage'}

    CON_RC = [
        'OK',  # 0
        'Connection refused - incorrect protocol version',  # 1
        'Connection refused - invalid client identifier',   # 2
        'Connection refused - server unavailable',  # 3
        'Connection refused - bad username or password',  # 4
        'Connection refused - not authorised',  # 5
    ]

    def __init__(self, topic, user, host=DEF_HOST, port=DEF_PORT,
                 debug=False):
        self._debug = debug
        self._log = get_logger(__class__.__name__, self._debug)
        self._log.debug('topic=%s', topic)
        self._log.debug('user=%s, host=%s, port=%d', user, host, port)

        if type(topic) == str:
            topic = [topic]

        if len(topic) == 0:
            return

        self._topics = topic
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

        self._log.debug('_user=\'%s\', _pw=\'%s\'', self._user, self._pw)

        self._mqttc = mqtt.Client()
        # self._mqttc.enable_logger()
        # self._mqttc.on_log = self.on_log
        self._mqttc.on_connect = self.on_connect
        self._mqttc.on_disconnect = self.on_disconnect
        self._mqttc.on_subscribe = self.on_subscribe
        self._mqttc.on_unsubscribe = self.on_unsubscribe
        self._mqttc.on_message = self.on_message
        self._mqttc.on_publish = self.on_publish

        self._mqttc.username_pw_set(self._user, self._pw)

        self._loop_active = False

    def start(self):
        self._log.debug('')

        self.loop_start()  # start thread
        ret = self.connect()

        self._log.debug('done: ret=%s', ret)
        return ret

    def end(self):
        self._log.debug('')

        self.disconnect()
        self.loop_stop()

        self._log.debug('done')

    def connect(self, keepalive=60):
        '''
        return: int
          0:   OK
          1-5: connect error
         -1:   unknown error
        '''
        self._log.debug('keepalive=%s', keepalive)
        self._mqttc.connect(self._svr_host, self._svr_port,
                            keepalive=keepalive)

        msg_type, msg_data = self.wait_msg(self.MSG_CON)
        if msg_type == self.MSG_CON:
            self._log.debug('rc=%d(%s), flag=%s',
                            msg_data['rc'],
                            self.CON_RC[msg_data['rc']],
                            msg_data['flag'])
            ret = msg_data['rc']
        else:
            self._log.debug('msg_type=%s, msg_data=%s', msg_type, msg_data)
            ret = -1

        self._log.debug('done: ret=%s', ret)
        return ret

    def disconnect(self):
        self._log.debug('')
        self._mqttc.disconnect()

        msg_type, msg_data = self.wait_msg(self.MSG_DISCON)
        if msg_data['rc'] != 0:
            self._log.warning('rc=%s !?', msg_data['rc'])
        self._log.debug('done: msg_type=%s, msg_data=%s', msg_type, msg_data)

    def send_data(self, topic, data):
        self._log.debug('topic=%s, data=%a', topic, data)

        self.publish(topic, data)

    def recv_data(self, topic):
        self._log.debug('topic=%s', topic)

        while self._loop_active:
            try:
                msg_type, msg_data = self.wait_msg(self.MSG_DATA)
                self._log.debug('msg_type=%s, msg_data=%s',
                                msg_type, msg_data)

                if msg_data['topic'] == topic:
                    return msg_data['payload']

            except Exception as e:
                self._log.debug('%s:%s', type(e).__name__, e)
                return None

            self.put_msg(msg_type, msg_data)
            time.sleep(random.random())

        self._log.info('done')
        return None

    def publish(self, topic, payload, qos=DEF_QOS, retain=False):
        self._log.debug('topic=%s, payload=%s, qos=%d, retain=%s',
                        topic, payload, qos, retain)

        if topic is None or topic == '':
            topic = self._topics[0]
            self._log.debug('topic=%s', topic)

        if type(topic) == int:
            topic = self._topics[topic]
            self._log.debug('topic=%s', topic)

        msg_payload = json.dumps(payload).encode('utf-8')
        self._log.debug('msg_payload=%s', msg_payload)

        self._mqttc.publish(topic, msg_payload, qos=qos, retain=retain)

        msg_type, msg_data = self.wait_msg(self.MSG_PUB)
        self._log.debug('done:msg_type=%s, msg_data=%s', msg_type, msg_data)

    def subscribe(self, topics=None, qos=DEF_QOS):
        self._log.debug('topics=%s, qos=%d', topics, qos)

        if topics is None:
            topics = self._topics
            self._log.debug('topics=%s', topics)

        if type(topics) != list:
            topics = [topics]

        for t in topics:
            self._mqttc.subscribe(t, qos=qos)
            t, d = self.wait_msg(self.MSG_SUB)
            self._log.debug('t=%s, d=%s', t, d)

    def unsubscribe(self, topic=None, ):
        self._log.debug('topic=%s', topic)

        if topic is None:
            topic = self._topics

        if type(topic) != list:
            topic = [topic]

        for t in self._topics:
            self._mqttc.unsubscribe(t)

    def wait_msg(self, wait_msg_type):
        self._log.debug('wait_msg_type=%s, _loop_active=%s',
                        wait_msg_type, self._loop_active)

        (msg_type, msg_data) = (self.MSG_NONE, None)

        while self._loop_active:
            msg_type, msg_data = self.get_msg(block=True, timeout=2)
            '''
            self._log.debug('wait_msgtype=%s, msg_type=%s, msg_data=%s',
                               wait_msg_type, msg_type, msg_data)
            '''

            if msg_type == wait_msg_type:
                self._log.debug('done:msg_type=%s, msg_data=%s',
                                msg_type, msg_data)
                return msg_type, msg_data

            if msg_type == self.MSG_ERR:
                self._log.debug('done:msg_type=%s, msg_data=%s',
                                msg_type, msg_data)
                return msg_type, msg_data

            if msg_type == self.MSG_NONE:
                continue

            self.put_msg(msg_type, msg_data)
            (msg_type, msg_data) = (self.MSG_NONE, None)
            time.sleep(random.random())

        self._log.debug('done:msg_type=%s, msg_data=%s', msg_type, msg_data)
        return msg_type, msg_data

    def loop_start(self):
        self._log.debug('')
        self._mqttc.loop_start()
        self._loop_active = True

    def loop_stop(self):
        self._log.debug('')
        self._loop_active = False
        self._mqttc.loop_stop()

    def put_msg(self, msg_type, msg_data):
        self._log.debug('msg_type=%s, msg_data=%s', msg_type, msg_data)

        msg = {'type': msg_type, 'data': msg_data}
        self._msgq.put(msg)

    def get_msg(self, block=False, timeout=None):
        try:
            msg = self._msgq.get(block=block, timeout=timeout)
        except queue.Empty:
            msg = {'type': self.MSG_NONE, 'data': None}

        self._log.debug('block=%s, timeout=%s, msg=%s', block, timeout, msg)
        return msg['type'], msg['data']

    def on_log(self, client, userdata, level, buf):
        self._log.debug('userdata=%s, level=%d, buf=%s',
                        userdata, level, buf)

    def on_connect(self, client, userdata, flag, rc):
        self._log.debug('userdata=%s, flag=%s, rc=%s', userdata, flag, rc)

        self.put_msg(self.MSG_CON, {'rc': rc, 'flag': flag})
        self._log.debug('done')

    def on_disconnect(self, client, userdata, rc):
        self._log.debug('userdata=%s, rc=%s', userdata, rc)

        if rc != 0:
            self.put_msg(self.MSG_ERR, 'disconnect error')
            return

        self.put_msg(self.MSG_DISCON, {'rc': rc})
        self._log.debug('done')

    def on_subscribe(self, client, userdata, mid, granted_qos):
        self._log.debug('userdata=%s, mid=%s, granted_qos=%s',
                        userdata, mid, granted_qos)
        self.put_msg(self.MSG_SUB, {'mid': mid, 'qos': granted_qos})
        self._log.debug('done')

    def on_unsubscribe(self, client, userdata, mid):
        self._log.debug('userdata=%s, mid=%s', userdata, mid)
        self.put_msg(self.MSG_UNSUB, {'mid': mid})
        self._log.debug('done')

    def on_message(self, client, userdata, msg):
        self._log.debug('userdata=%s, msg=%s', userdata, msg)

        topic = msg.topic
        payload = json.loads(msg.payload.decode('utf-8'))
        self._log.debug('topic=%s, payload=%s', topic, payload)

        msg_data = {'topic': topic, 'payload': payload}
        self.put_msg(self.MSG_DATA, msg_data)
        self._log.debug('done')

    def on_publish(self, client, userdata, mid):
        self._log.debug('userdata=%s, mid=%s', userdata, mid)
        self.put_msg(self.MSG_PUB, {'mid': mid})
        self._log.debug('done')

    def load_conf(self):
        self._log.debug('')

        conf_file = self.find_conf()
        self._log.debug('conf_file=%s', conf_file)
        if conf_file is None:
            return None

        with open(conf_file, 'r') as f:
            csv_reader = csv.reader(f, skipinitialspace=True, quotechar='"')
            for row in csv_reader:
                # self._log.debug('row=%s', row)
                if row[0].startswith('#'):
                    continue
                while len(row) < 5:
                    row.append('')
                conf_ent = {'host': row[0], 'port': int(row[1]),
                            'topic': row[2],
                            'user': row[3], 'pw': row[4]}
                # self._log.debug('conf_ent=%s', conf_ent)
                self._conf.append(conf_ent)

        self._log.debug('done: _conf=%s', self._conf)
        return self._conf

    def find_conf(self):
        self._log.debug('')

        for dir in self.CONF_PATH:
            for fname in self.CONF_FILENAME:
                pathname = dir + '/' + fname
                if os.path.isfile(pathname) or os.path.islink(pathname):
                    return pathname
        return None

    def get_pw(self, user):
        """
        topicが複数の場合は、先頭のトピックがマッチする
        """
        self._log.debug('user=%s', user)
        self._log.debug('host=%s, port=%s, topic=%s',
                        self._svr_host, self._svr_port, self._topics[0])

        for cf in self._conf:
            # self._log.debug('cf=%s', cf)
            if cf['host'] == self._svr_host and \
               cf['port'] == self._svr_port and \
               cf['topic'] == self._topics[0] and \
               cf['user'] == user:
                pw = cf['pw']
                self._log.debug('user=%s, pw=%s', user, pw)
                return pw

        return None


class MqttApp:
    def __init__(self, mqtt, debug=False):
        self._debug = debug
        self._log = get_logger(__class__.__name__, self._debug)
        self._log.info('mqtt=%s', type(mqtt))

        self._mqtt = mqtt
        self._topic = self._mqtt._topics[0]
        self._active = False

        self._th = threading.Thread(target=self.receiver)

    def main(self):
        self._log.info('')

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
            self._log.debug('data=%a', data)

            self._mqtt.send_data(self._topic, data)

        self._log.info('done')

    def end(self):
        self._log.info('')
        self._active = False

        self._log.info('_mqtt.end() ..')
        self._mqtt.end()

        self._log.info('_th.join() ..')
        self._th.join()

        self._log.info('done')

    def receiver(self):
        self._log.debug('')

        while self._active:
            data = self._mqtt.recv_data(self._topic)
            self._log.debug('data=%s', data)
            if data is None:
                continue

            print('> %a' % (data))

        self._log.info('done')


class MqttServerApp:
    def __init__(self, mqtt, debug=False):
        self._debug = debug
        self._log = get_logger(__class__.__name__, self._debug)
        self._log.debug('mqtt=%s', type(mqtt))

        self._mqtt = mqtt

        self._topics = self._mqtt._topics
        if len(self._topics) < 2:
            raise RuntimeError('len(_topics) < 2: %s' % self._topics)

        self._topic_request = self._topics[0]
        self._topic_reply = self._topics[1]

    def main(self):
        self._log.debug('')

        ret = self._mqtt.start()
        if ret != 0:
            raise RuntimeError('start(): ret=%s' % (ret))

        self._mqtt.subscribe(self._topic_request)

        self._active = True

        while self._active:
            data = self._mqtt.recv_data(self._topic_request)
            self._log.info('recv[%s]: data="%s"', self._topic_request, data)

            th = threading.Thread(target=self.handle, args=([data]))
            th.start()

        self._log.debug('done')

    def end(self):
        self._log.info('')
        self._active = False
        self._log.info('done')

    def handle(self, data):
        time.sleep(1)

        self._mqtt.send_data(self._topic_reply, data)
        self._log.info('send[%s]: data="%s"', self._topic_reply, data)


class MqttClientApp:
    def __init__(self, mqtt, debug=False):
        self._debug = debug
        self._log = get_logger(__class__.__name__, self._debug)
        self._log.debug('mqtt=%s', type(mqtt))

        self._mqtt = mqtt

        self._topics = self._mqtt._topics
        if len(self._topics) < 2:
            raise RuntimeError('len(_topics) <2: %s' % self._topics)

        self._topic_request = self._topics[0]
        self._topic_reply = self._topics[1]

        self._mqtt.start()
        self._mqtt.subscribe(self._topic_reply)

        self.th = threading.Thread(target=self.receiver)

        self._active = True

    def main(self):
        self._log.debug('')
        self.th.start()

        while self._active:
            data = input('--==< OK >==--\n')
            self._log.debug('data="%s"', data)
            if data == '':
                self._active = False
                break

            self._mqtt.send_data(self._topic_request, data)

        self._log.debug('done')

    def receiver(self):
        self._log.debug('')

        while self._active:
            data = self._mqtt.recv_data(self._topic_reply)
            print('"%s"' % (data))

        self._log.debug('done')

    def end(self):
        self._log.debug('')
        self._active = False

        self._mqtt.end()

        self.th.join()
        self._log.debug('done')


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
    log = get_logger(__name__, debug=debug)

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
        log.info('finally')
        app.end()
        log.info('done')


if __name__ == '__main__':
    main()
