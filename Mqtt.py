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

    _log = get_logger(__name__, False)
    # _log.info('%s', __name__)  # for debug
    
    def __init__(self, cb_recv=None, topics_sub=None,
                 user='', pw='', host=DEF_HOST, port=DEF_PORT,
                 debug=False):
        self._dbg = debug
        __class__._log = get_logger(__class__.__name__, self._dbg)
        self._log.debug('cb_recv=%s, topics_sub=%s', cb_recv, topics_sub)
        self._log.debug('user=%s, pw=%s, host=%s, port=%s',
                        user, pw, host, port)

        if cb_recv == self.CB_QPUT:
            cb_recv = self.cb_qput
            self._log.debug('cb_recv=%s', self._cb_recv)
        self._cb_recv = cb_recv

        if type(topics_sub) != list:
            topics_sub = [topics_sub]
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

    def send_data(self, data, topics, qos=0, retain=False):
        self._log.debug('data=%a, topics=%s', data, topics)

        if type(topics) != list:
            topics = [ topics ]
            self._log.debug('topics=%s', topics)

        payload = json.dumps(self.data2payload(data)).encode('utf-8')
        self._log.debug('payload=%a', payload)

        for t in topics:
            if t is None or t == '':
                self._log.debug('t=\'%s\': ** ignore **', t)
                continue

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

        if rc != 0:
            self._log.error('rc=%s (flag=%s)', rc, flag)
            return

        # subscribe
        topics = []
        for t in self._topics_sub:
            if t is None or t == '':
                self._log.debug('t=%a: ** ignore **', t)
                continue
            topics.append((t, 0))
        if len(topics) >= 1:
            ret = self._mqttc.subscribe(topics)
            self._log.debug('subscribe(%s) ==> ret=%s', topics, ret)

    def _on_disconnect(self, client, userdata, rc):
        self._log.debug('userdata=%s, rc=%s', userdata, rc)
        if rc != 0:
            self._log.error('rc=%s', rc)

    def _on_subscribe(self, client, userdata, mid, granted_qos):
        self._log.debug('userdata=%s, mid=%s, granted_qos=%s',
                        userdata, mid, granted_qos)

        err = False
        for q in granted_qos:
            if q > 3:
                err=True

        if err:
            self._log.error('granted_qos=%s', granted_qos)

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
    _log = get_logger(__name__, False)

    def __init__(self, user='', pw='',
                 host=Mqtt.DEF_HOST, port=Mqtt.DEF_PORT,
                 debug=False):
        self._dbg = debug
        __class__._log = get_logger(__class__.__name__, self._dbg)
        self._log.debug('user=%s, pw=%s, host=%s, port=%s',
                        user, pw, host, port)

        super().__init__(None, None, user, pw, host, port, self._dbg)


class Beebotte(Mqtt):
    '''
    Beebltteでは、
    * topicsは、``channel/resource``の形式。
    * ``token``は、``channel``毎

    従って、``topics_sub``などは、全て同じ``channel``でなければならない。
    '''
    BEEBOTTE_HOST = 'mqtt.beebotte.com'
    BEEBOTTE_PORT = 1883

    _log = get_logger(__name__, False)

    def __init__(self, cb_recv=None, topics_sub=None, token='', debug=False):
        self._dbg = debug
        __class__._log = get_logger(__class__.__name__, self._dbg)
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

    @classmethod
    def ts2datestr(cls, ts_msec):
        cls._log.debug('ts_msec=%d', ts_msec)

        datestr = time.strftime('%Y/%m/%d,%H:%M:%S',
                                time.localtime(ts_msec / 1000))
        return datestr


class BeebotteSubscriber(Beebotte):
    '''
    exactly same as Beebotte class
    '''
    pass


class BeebottePublisher(Beebotte):
    _log = get_logger(__name__, False)

    def __init__(self, token='', debug=False):
        self._dbg = debug
        __class__._log = get_logger(__class__.__name__, self._dbg)
        self._log.debug('token=%s', token)

        super().__init__(None, [], token, debug=self._dbg)


class App:
    _log = get_logger(__name__, False)

    STR_EXIT = ['exit', 'quit']

    def __init__(self, topic_send, topic_recv, user, password, svr_host,
                 beebotte, debug=False):
        self._dbg = debug
        __class__._log = get_logger(__class__.__name__, self._dbg)
        self._log.debug('topic_send=%s, topic_recv=%s',
                        topic_send, topic_recv)
        self._log.debug('user=%s, password=%s', user, password)
        self._log.debug('svr_host=%s, beebotte=%s', svr_host, beebotte)

        self._topic_send = topic_send
        self._topic_recv = topic_recv
        self._user = user
        self._password = password
        self._svr_host = svr_host
        self._beebotte = beebotte

        if self._beebotte:
            self._svr = Beebotte(self.cb, [topic_recv], user, debug=self._dbg)
        else:
            self._svr = Mqtt(self.cb, [topic_recv], user, password, svr_host,
                             debug=self._dbg)

    def cb(self, data, topic, ts):
        if self._beebotte:
            ts = self._svr.ts2datestr(ts)
        print('%s [%s] %s' % (ts, topic, data))

        if data in self.STR_EXIT:
            self._svr.active = False

    def main(self):
        self._svr.start()

        while self._svr.active:
            data = input('>> Ready <<\n')
            self._svr.send_data(data, self._topic_send)
            if data in self.STR_EXIT:
                break

    def end(self):
        self._svr.end()


@click.command(context_settings=CONTEXT_SETTINGS, help='MQTT Sample')
@click.argument('topic_recv', type=str, default='')
@click.argument('user', type=str, default='')
@click.option('--topic_send', '-ts', '-t', 'topic_send', type=str, default='',
              help='topic to send')
@click.option('--password', '-p', 'password', type=str, default='',
              help='password')
@click.option('--svr_host', '-s', 'svr_host', type=str,
              default='mqtt.beebotte.com',
              help='server host name')
@click.option('--beebotte', '-b', 'beebotte', is_flag=True, default=False,
              help='Beebotte flag')
@click.option('--debug', '-d', 'debug', is_flag=True, default=False,
              help='debug flag')
def main(topic_send, topic_recv, user, password, svr_host, beebotte, debug):
    log = get_logger(__name__, debug=debug)
    log.debug('topic_send=%s, topic_recv=%s', topic_send, topic_recv)
    log.debug('user=%s, password=%s', user, password)
    log.debug('svr_host=%s, beebotte=%s', svr_host, beebotte)

    app = App(topic_send, topic_recv, user, password, svr_host, beebotte,
              debug=debug)
    try:
        app.main()
    finally:
        log.debug('finally')
        app.end()
        log.debug('done')


if __name__ == '__main__':
    main()
