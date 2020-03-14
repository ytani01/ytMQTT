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
import click
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


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

    def __init__(self, user, pw, host=DEF_HOST, port=DEF_PORT,
                 debug=False):
        self._debug = debug
        self._log = get_logger(__class__.__name__, self._debug)
        self._log.debug('user=%s, pw=%s, host=%s, port=%d',
                        user, pw, host, port)

        self._user = user
        self._pw = pw
        self._svr_host = host
        self._svr_port = port

        self._subsc_topics = []
        self._msgq = queue.Queue()

        self._connecting = False
        self._disconnecting = False
        self._subscribing = False

        self._mqttc = mqtt.Client()
        self._mqttc.username_pw_set(self._user, self._pw)

        # self._mqttc.enable_logger()
        # self._mqttc.on_log = self.on_log
        self._mqttc.on_connect = self.on_connect
        self._mqttc.on_disconnect = self.on_disconnect
        self._mqttc.on_subscribe = self.on_subscribe
        self._mqttc.on_unsubscribe = self.on_unsubscribe
        self._mqttc.on_publish = self.on_publish
        self._mqttc.on_message = self.on_message

        self._loop_active = False

    def start(self):
        self._log.debug('')

        self._loop_active = True
        self._mqttc.loop_start()
        ret = self.connect()

        self._log.debug('done: ret=%s', ret)
        return ret

    def end(self):
        self._log.debug('')

        self._loop_active = False
        self.disconnect()
        self._mqttc.loop_stop()

        self._log.debug('done')

    def connect(self, keepalive=60):
        '''
        return: int
          0:   OK
          1-5: connect error
         -1:   unknown error
         -2:   subscribe() error
        '''
        self._log.debug('keepalive=%s', keepalive)

        self._connecting = True
        self._mqttc.connect(self._svr_host, self._svr_port,
                            keepalive=keepalive)

        t, d = self.wait_msg(self.MSG_CON)
        self._log.debug('t=%s, d=%s', t, d)
        if t == self.MSG_CON:
            ret = d['rc']
            self._connecting = False
        else:
            ret = -1

        if ret != 0:
            return ret

        if len(self._subsc_topics) > 0:
            self._subscribing = True
            t, d = self.wait_msg(self.MSG_SUB)
            self._log.debug('%s, %s', t, d)
            if t == self.MSG_SUB:
                self._subscribing = False
                for q in d['qos']:
                    if q > 2:
                        self._log.error('subscribe(%s): failed, d[\'qos\']:%s',
                                        self._subsc_topics, d['qos'])
                        return -2

        self._log.debug('done: ret=%s', ret)
        return ret

    def disconnect(self):
        self._log.debug('')

        self._disconnecting = True
        self._mqttc.disconnect()

        t, d = self.wait_msg(self.MSG_DISCON)
        if t == self.MSG_DISCON:
            self._disconnecting = False
            if d['rc'] != 0:
                self._log.warning('rc=%s !?', d['rc'])

        self._conn_active = 0

        self._log.debug('done: (%s, %s)', t, d)
        return t, d

    def send_data(self, topic, data):
        self._log.debug('topic=%s, data=%a', topic, data)

        t, d = self.publish(topic, data)

        self._log.debug('done: (%s, %s)', t, d)
        return t, d

    def recv_data(self, topic):
        self._log.debug('topic=%s', topic)

        while self._loop_active:
            try:
                t, d = self.wait_msg(self.MSG_DATA)
                self._log.debug('t=%s, d=%s', t, d)

                if d['topic'] == topic:
                    self._log.debug('done .. return %s', d['payload'])
                    return d['payload']

            except Exception as e:
                self._log.debug('%s:%s .. return None', type(e).__name__, e)
                return None

            self.put_msg(t, d)
            time.sleep(random.random())

        self._log.info('done: _loop_active=%s .. return None',
                       self._loop_active)
        return None

    def publish(self, topic, payload, qos=DEF_QOS, retain=False):
        self._log.debug('topic=%s, payload=%s, qos=%d, retain=%s',
                        topic, payload, qos, retain)

        msg_payload = json.dumps(payload).encode('utf-8')
        self._log.debug('msg_payload=%s', msg_payload)

        ret = self._mqttc.publish(topic, msg_payload, qos=qos, retain=retain)
        if ret.rc != 0:
            self._log.error('_mqttc.publish(%s): failed(%s)', topic, ret)

        t, d = self.wait_msg(self.MSG_PUB)
        self._log.debug('done: (%s, %s)', t, d)
        return t, d

    def set_subscribe(self, topics):
        self._log.debug('topics=%s', topics)
        self._subsc_topics = topics

    def do_subscribe(self, topics, qos=DEF_QOS):
        self._log.debug('topics=%s, qos=%d', topics, qos)

        if type(topics) != list:
            topics = [topics]
            self._log.debug('topics=%s', topics)

        if len(topics) == 0:
            self._log.warning('do nothing')
            return

        topics2 = [(t, qos) for t in topics]
        self._log.debug('topics2=%s', topics2)

        self._mqttc.subscribe(topics2)

        self._log.debug('done')

    def unsubscribe(self, topics=None):
        self._log.debug('topics=%s', topics)

        if topics is None:
            topics = self._subsc_topics
            self._log.debug('topics=%s', topics)

        if type(topics) != list:
            topics = [topics]
            self._log.debug('topics=%s', topics)

        self._mqttc.unsubscribe(topics)
        t, d = self.wait_msg(self.MSG_UNSUB)
        self._log.debug('done: (%s, %s)', t, d)

    def wait_msg(self, wait_msg_type):
        self._log.debug('wait_msg_type=%s, _loop_active=%s',
                        wait_msg_type, self._loop_active)

        (t, d) = (self.MSG_NONE, None)

        while self._loop_active:
            t, d = self.get_msg(timeout=2)

            if t == wait_msg_type:
                self._log.debug('done: (%s, %s)', t, d)
                return t, d

            # t != wait_msg_type

            '''
            if wait_msg_type == self.MSG_DATA:
                # if t == self.MSG_DISCON or t == self.MSG_CON
                if t in  [self.MSG_DISCON, self.MSG_CON,
                          self.MSG_SUB]:
                    self._log.warning('Ignored: %s, %s', t, d)
                    continue
            '''

            if t == self.MSG_CON:
                if not self._connecting:
                    self._log.warning('_connecting=%s .. Ignored: %s, %s',
                                      self._connecting, t, d)
                    continue

            if t == self.MSG_DISCON:
                if not self._disconnecting:
                    self._log.warning('_disconnecting=%s .. Ignored: %s, %s',
                                      self._disconnecting, t, d)
                    continue

            if t == self.MSG_SUB:
                if not self._subscribing:
                    self._log.warning('_subscribing=%s .. Ignored: %s, %s',
                                      self._subscribing, t, d)
                    continue

            if t == self.MSG_ERR:
                self._log.debug('done: (%s, %s)', t, d)
                return t, d

            if t == self.MSG_NONE:
                continue

            sleep_sec = random.random()
            self._log.debug('waiting %s .. Skip: %s, %s .. sleep %.2f sec ..',
                            wait_msg_type, t, d, sleep_sec)
            self.put_msg(t, d)
            (t, d) = (self.MSG_NONE, None)
            time.sleep(random.random())

        self._log.debug('done: (%s, %s)', t, d)
        return t, d

    def put_msg(self, msg_type, msg_data):
        self._log.debug('msg_type=%s, msg_data=%s', msg_type, msg_data)
        
        msg = {'type': msg_type, 'data': msg_data}
        self._log.debug('%s', msg)
        self._msgq.put(msg)

    def get_msg(self, block=True, timeout=None):
        # self._log.debug('block=%s, timeout=%s', block, timeout)
        try:
            msg = self._msgq.get(block=block, timeout=timeout)
        except queue.Empty:
            msg = {'type': self.MSG_NONE, 'data': None}

        # self._log.debug('%s', msg)
        return msg['type'], msg['data']

    def on_log(self, client, userdata, level, buf):
        self._log.debug('userdata=%s, level=%d, buf=%s',
                        userdata, level, buf)

    def on_connect(self, client, userdata, flag, rc):
        self._log.debug('userdata=%s, flag=%s, rc=%s', userdata, flag, rc)

        ret = self.do_subscribe(self._subsc_topics)
        self._log.debug('do_subscript(%s) ==> %s', self._subsc_topics, ret)

        self.put_msg(self.MSG_CON, {'rc': rc, 'flag': flag})
        self._log.debug('done')

    def on_disconnect(self, client, userdata, rc):
        self._log.debug('userdata=%s, rc=%s', userdata, rc)

        '''
        if rc != 0:
            self.put_msg(self.MSG_ERR, 'disconnect error')
            return
        '''

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
        self._log.debug('userdata=%s, msg.payload=%s', userdata, msg.payload)

        topic = msg.topic
        self._log.debug('topic=%s', topic)
        try:
            payload = json.loads(msg.payload.decode('utf-8'))
        except Exception as e:
            self._log.warning('%s:%s', type(e).__name__, e)
            
        self._log.debug('payload=%s', payload)

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


class MqttApp:
    def __init__(self, user, pw, host, port, topic, debug=False):
        self._debug = debug
        self._log = get_logger(__class__.__name__, self._debug)
        self._log.debug('user=%s, pw=%s, host=%s, port=%s, topic=%s',
                        user, pw, host, port, topic)

        self._mqtt = Mqtt(user, pw, host, port, debug=self._debug)
        self._topic = topic

        self._active = False
        self._th = threading.Thread(target=self.receiver)

    def main(self):
        self._log.debug('')

        self._mqtt.set_subscribe(self._topic)
        if self._mqtt.start() != 0:
            self._log.error('start(): failed')
            return

        self._active = True
        self._th.start()

        while self._active:
            data = input('--==< Ready >==--\n')

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

        self._log.debug('done')

    def end(self):
        self._log.debug('')
        self._active = False

        self._mqtt.unsubscribe()

        self._log.debug('_mqtt.end() ..')
        self._mqtt.end()

        if self._th.is_alive():
            self._log.debug('_th.join() ..')
            self._th.join()

        self._log.debug('done')

    def receiver(self):
        self._log.debug('')

        while self._active:
            data = self._mqtt.recv_data(self._topic)
            self._log.debug('data=%s', data)
            if data is None:
                continue

            print('> %a' % (data))

        self._log.debug('done')


class MqttServerApp:
    def __init__(self, user, pw, host, port, topic_request, topic_reply,
                 debug=False):
        self._debug = debug
        self._log = get_logger(__class__.__name__, self._debug)
        self._log.debug('user=%s, pw=%s, host=%s, port=%s,',
                        user, pw, host, port)
        self._log.debug('topic_request=%s, topic_reply=%s',
                        topic_request, topic_reply)

        self._mqtt = Mqtt(user, pw, host, port, debug=self._debug)
        self._topic_request = topic_request
        self._topic_reply = topic_reply

    def main(self):
        self._log.debug('')

        self._mqtt.set_subscribe(self._topic_request)
        ret = self._mqtt.start()
        if ret != 0:
            self._log.error('start(): failed')
            return

        self._active = True
        self._log.info('Ready')

        while self._active:
            data = self._mqtt.recv_data(self._topic_request)
            self._log.info('recv[%s]: data="%s"', self._topic_request, data)

            #
            # single thread version
            #
            self.handle(data)

            '''

            #
            # multi thread version
            #
            th = threading.Thread(target=self.handle, args=([data]))
            th.start()
            '''

        self._log.debug('done')

    def end(self):
        self._log.info('')
        self._active = False

        self._mqtt.unsubscribe(self._topic_request)

        self._log.debug('_mqtt.end() ..')
        self._mqtt.end()

        self._log.info('done')

    def handle(self, data):
        time.sleep(2)  # dummy

        self._log.info('send[%s]: data="%s"', self._topic_reply, data)
        self._mqtt.send_data(self._topic_reply, data)


class MqttClientApp:
    def __init__(self, user, pw, host, port, topic_request, topic_reply,
                 debug=False):
        self._debug = debug
        self._log = get_logger(__class__.__name__, self._debug)
        self._log.debug('user=%s, pw=%s, host=%s, port=%s,',
                        user, pw, host, port)
        self._log.debug('topic_request=%s, topic_reply=%s',
                        topic_request, topic_reply)

        self._mqtt = Mqtt(user, pw, host, port, debug=self._debug)
        self._topic_request = topic_request
        self._topic_reply = topic_reply

        self._th = threading.Thread(target=self.receiver)

        self._active = True

    def main(self):
        self._log.debug('')

        self._mqtt.set_subscribe(self._topic_reply)
        ret = self._mqtt.start()
        if ret != 0:
            self._log.error('start(): faild')
            return

        self._th.start()

        while self._active:
            data = input('--==< OK >==--\n')
            self._log.debug('data="%s"', data)
            if data == '':
                self._active = False
                break

            self._mqtt.send_data(self._topic_request, data)

        self._log.debug('done')

    def end(self):
        self._log.debug('')
        self._active = False

        self._mqtt.unsubscribe(self._topic_reply)

        self._log.debug('_mqtt.end() ..')
        self._mqtt.end()

        if self._th.is_alive():
            self._log.debug('join ..')
            self._th.join()

        self._log.debug('done')

    def receiver(self):
        self._log.debug('')

        while self._active:
            data = self._mqtt.recv_data(self._topic_reply)
            if data is not None:
                print('> %a' % (data))

        self._log.debug('done')


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

    app = None

    if mode == '':
        app = MqttApp(user, '', mqtt_host, mqtt_port, topic[0], debug=debug)

    if mode != '':
        if len(topic) != 2:
            print('topics must be .. {request topic} {reply topic}')
            return

    if mode == 'c':
        app = MqttClientApp(user, '', mqtt_host, mqtt_port,
                            topic[0], topic[1], debug=debug)

    if mode == 's':
        if topic[0] == topic[1]:
            print('topics must be .. {request topic} {reply topic}')
            return
        app = MqttServerApp(user, '', mqtt_host, mqtt_port,
                            topic[0], topic[1], debug=debug)

    if app is None:
        return

    try:
        app.main()
    finally:
        log.debug('finally')
        app.end()
        log.debug('done')


if __name__ == '__main__':
    main()
