#!/usr/bin/env python3
#
# (C) 2020 Yoichi Tanibayashi
#
__author__ = 'Yoichi Tanibayashi'
__date__   = '2020'

import paho.mqtt.client as mqtt
import time
import json
import queue
from MyLogger import get_logger
import click
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


class Mqtt:
    '''
    topics_sub = [ topic1, topic2 .. ]
    _cb_recv(data, topic, ts)
    
    キューを介して同期的にデータ受信する場合は、
    _cb_recvに``Mqtt.CB_QPUT``を指定して、
    ``recv_data()`` で受信する。

    '''
    DEF_HOST = 'mqtt.beebotte.com'
    DEF_PORT = 1883
    CB_QPUT = '__Q_PUT__'

    def __init__(self, cb_recv=None, topics_sub=[],
                 user='', pw='', host=DEF_HOST, port=DEF_PORT,
                 debug=False):
        self._dbg = debug
        self._log = get_logger(__class__.__name__, self._dbg)
        self._log.debug('cb_recv=%s, topics_sub=%s', cb_recv, topics_sub)
        self._log.debug('user=%s, pw=%s, host=%s, port=%s',
                        user, pw, host, port)

        self._cb_recv = cb_recv
        if self._cb_recv == self.CB_QPUT:
            self._cb_recv = self.cb_qput
            self._log.debug('cb_recv=%s', self._cb_recv)
        self._topics_sub = topics_sub
        self._user = user
        self._pw = pw
        self._host = host
        self._port = port

        if type(self._topics_sub) != list:
            self._topics_sub = [ self._topics_sub ]
            self._log.warning('self._topics_sub=%s', self._topics_sub)

        self._dataq = queue.Queue()

        self._mqttc = mqtt.Client()
        self._mqttc.enable_logger()
        self._mqttc.username_pw_set(self._user, self._pw)

        self._mqttc.on_connect = self._on_connect
        self._mqttc.on_disconnect = self._on_disconnect
        self._mqttc.on_subscribe = self._on_subscribe
        self._mqttc.on_unsubscribe = self._on_unsubscribe
        self._mqttc.on_publish = self._on_publish
        self._mqttc.on_message = self._on_message

        self.active = False

    def start(self):
        self._log.debug('')

        ret = self._mqttc.connect(self._host, self._port, keepalive=60)
        self._log.debug('ret=%s', ret)

        self._mqttc.loop_start()
        self.active = True

    def end(self):
        self._log.debug('')

        self.active = False

        self._mqttc.disconnect()
        # time.sleep(1)
        self._mqttc.loop_stop()

        self._log.debug('done')

    def send_data(self, data, topics=[], qos=0, retain=False):
        self._log.debug('data=%a, topics=%s', data, topics)

        if type(topics) != list:
            topics = [ topics ]
            self._log.warning('topics=%s', topics)

        payload = json.dumps(self.data2payload(data)).encode('utf-8')
        self._log.debug('payload=%a', payload)

        for t in topics:
            ret = self._mqttc.publish(t, payload,
                                      qos=qos, retain=retain)
            self._log.debug('publish(%s) ==> ret=%s', t, ret)

    def cb_qput(self, data, topic, ts):
        self._log.debug('data=%s, topic=%s, ts=%s', data, topic, ts)
        self._dataq.put((data, topic, ts))

    def recv_data(self, timeout=2):
        '''
        retuen: (data, topic, ts)
        '''
        self._log.debug('timeout=%s', timeout)

        while self.active:
            try:
                ret = self._dataq.get(timeout=timeout)
                self._log.debug('ret=%s', ret)
                return ret

            except queue.Empty as e:
                self._log.debug('%s:%s', type(e).__name__, e)

        return None

    def data2payload(self, data):
        return data

    def payload2data(self, payload):
        return payload

    def get_ts(self, msg, payload):
        return msg.timestamp

    def _on_message(self, client, userdata, msg):
        self._log.debug('userdata=%s', userdata)
        self._log.debug('msg.topic=%s', msg.topic)

        payload = json.loads(msg.payload.decode('utf-8'))
        self._log.debug('payload=%s', payload)

        data = self.payload2data(payload)
        self._log.debug('data=%s', data)

        ts = self.get_ts(msg, payload)
        self._log.debug('ts=%s', ts)

        if self._cb_recv is not None:
            self._cb_recv(data, msg.topic, ts)

    def _on_connect(self, client, userdata, flag, rc):
        self._log.debug('userdata=%s, flag=%s, rc=%s', userdata, flag, rc)

        self._log.debug('_topics_sub=%s', self._topics_sub)
        if self._topics_sub != []:
            topics = [(t, 0) for t in self._topics_sub]
            ret = self._mqttc.subscribe(topics)
            self._log.debug('subscribe(%s) ==> ret=%s', topics, ret)

    def _on_disconnect(self, client, userdata, rc):
        self._log.debug('userdata=%s, rc=%s', userdata, rc)

    def _on_subscribe(self, client, userdata, mid, granted_qos):
        self._log.debug('userdata=%s, mid=%s, granted_qos=%s',
                        userdata, mid, granted_qos)

    def _on_unsubscribe(self, client, userdata, mid):
        self._log.debug('userdata=%s, mid', userdata, mid)

    def _on_publish(self, client, userdata, mid):
        self._log.debug('userdata=%s, mid=%s', userdata, mid)


class MqttSubscriber(Mqtt):
    '''
    exactly same as Mqtt class
    '''
    pass


class MqttPublisher(Mqtt):
    def __init__(self, user='', pw='',
                 host=Mqtt.DEF_HOST, port=Mqtt.DEF_PORT,
                 debug=False):
        self._dbg = debug
        self._log = get_logger(__class__.__name__, self._dbg)
        self._log.debug('user=%s, pw=%s, host=%s, port=%s',
                        user, pw, host, port)

        super().__init__(None, [], user, pw, host, port, self._dbg)


class Beebotte(Mqtt):
    '''
    Beebltteでは、
    * topicsは、``channel/resource``の形式。
    * ``token``は、``channel``毎

    従って、``topics_sub``などは、全て同じ``channel``でなければならない。
    '''
    BEEBOTTE_HOST = 'mqtt.beebotte.com'
    BEEBOTTE_PORT = 1883

    def __init__(self, cb_recv=None, topics_sub=[], token='', debug=False):
        self._dbg = debug
        self._log = get_logger(__class__.__name__, self._dbg)
        self._log.debug('topics_sub=%s, token=%s', topics_sub, token)

        super().__init__(cb_recv, topics_sub, token, '',
                         self.BEEBOTTE_HOST, self.BEEBOTTE_PORT,
                         debug=self._dbg)

    def data2payload(self, data):
        self._log.debug('data=%s', data)

        ts = int(time.time() * 1000)
        payload = {'data': data, 'ts': ts, 'ispublic': False}
        self._log.debug('payload=%s', payload)
        return payload

    def payload2data(self, payload):
        self._log.debug('payload=%s', payload)
        return payload['data']

    def get_ts(self, msg, payload):
        ts = payload['ts']
        self._log.debug('ts=%s', ts)
        return ts

    def ts2datestr(self, ts_msec):
        self._log.debug('ts_msec=%d', ts_msec)

        datestr = time.strftime('%Y/%m/%d,%H:%M:%S',
                                time.localtime(ts_msec / 1000))
        return datestr


class BeebotteSubscriber(Beebotte):
    '''
    exactly same as Beebotte class
    '''
    pass


class BeebottePublisher(Beebotte):
    def __init__(self, token='', debug=False):
        self._dbg = debug
        self._log = get_logger(__class__.__name__, self._dbg)
        self._log.debug('token=%s', token)

        super().__init__(None, [], token, debug=self._dbg)


class App:
    def __init__(self, topic_request, topic_reply, token, debug=False):
        self._dbg = debug
        self._log = get_logger(__class__.__name__, self._dbg)
        self._log.debug('topic_request=%s, topic_reply=%s, token=%s',
                        topic_request, topic_reply, token)

        self._topic_reply = topic_reply
        self._svr = Beebotte(self.cb, [topic_request], token, debug=self._dbg)

    def cb(self, data, topic, ts):
        self._log.info('data=%a topic=%s, ts=%s', data, topic, ts)

        if data == 'exit':
            self.svr.active = False

        self._svr.send_data('>>%s<<' % data, [self._topic_reply])

    def main(self):
        self._svr.start()

        while self._svr.active:
            print('Sleep ..')
            time.sleep(5)

    def end(self):
        self._svr.end()


@click.command(context_settings=CONTEXT_SETTINGS, help='''
MQTT Simple Server App
''')
@click.argument('topic_request', type=str)
@click.argument('topic_reply', type=str)
@click.argument('token', type=str)
@click.option('--debug', '-d', 'debug', is_flag=True, default=False,
              help='debug flag')
def main(topic_request, topic_reply, token, debug):
    log = get_logger(__name__, debug=debug)

    app = App(topic_request, topic_reply, token, debug=debug)
    try:
        app.main()
    finally:
        log.debug('finally')
        app.end()
        log.debug('done')


if __name__ == '__main__':
    main()
