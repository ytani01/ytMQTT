#!/usr/bin/env python3
#
# (C) 2020 Yoichi Tanibayashi
#
__author__ = 'Yoichi Tanibayashi'
__date__   = '2020'

import paho.mqtt.client as mqtt
import time
import json
from MyLogger import get_logger
import click
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


class MqttClientServer:
    DEF_HOST = 'mqtt.beebotte.com'
    DEF_PORT = 1883

    def __init__(self, cb_func=None, topic_request=None, topic_reply=None,
                 user='', pw='', host=DEF_HOST, port=DEF_PORT,
                 debug=False):
        self._dbg = debug
        self._log = get_logger(__class__.__name__, self._dbg)
        self._log.debug('topic_request=%s, topic_reply=%s',
                        topic_request, topic_reply)
        self._log.debug('user=%s, pw=%s, host=%s, port=%s',
                        user, pw, host, port)

        self._cb_func = cb_func
        self._topic_request = topic_request
        self._topic_reply = topic_reply
        self._user = user
        self._pw = pw
        self._host = host
        self._port = port

        if self._topic_request == '':
            self._topic_request = None
        if self._topic_request is not None:
            if type(self._topic_request) != list:
                self._topic_request = [self._topic_request]
                self._log.debug('_topic_request=%s', self._topic_request)

        if self._topic_reply == '':
            self._topic_reply = None
        if self._topic_reply is not None:
            if type(self._topic_reply) != list:
                self._topic_reply = [self._topic_reply]
                self._log.debug('_topic_reply=%s', self._topic_reply)

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

    def send_data(self, data, qos=0, retain=False):
        self._log.debug('data=%a', data)

        if self._topic_reply is None:
            self._log.debug('_topic_reply=%s .. do nothing',
                            self._topic_reply)
            return

        payload = json.dumps(self.data2payload(data)).encode('utf-8')

        for t in self._topic_reply:
            ret = self._mqttc.publish(t, payload,
                                      qos=qos, retain=retain)
            self._log.debug('publish() ==> ret=%s', ret)

    def data2payload(self, data):
        return data

    def payload2data(self, payload):
        return payload

    def get_ts(self, msg, payload):
        return msg.timestamp

    def _on_connect(self, client, userdata, flag, rc):
        self._log.debug('userdata=%s, flag=%s, rc=%s', userdata, flag, rc)

        self._log.debug('_topic_request=%s', self._topic_request)
        if self._topic_request is not None:
            topics = [(t, 0) for t in self._topic_request]
            ret = self._mqttc.subscribe(topics)
            self._log.debug('subscribe(%s) ==> ret=%s',
                            self._topic_request, ret)

    def _on_disconnect(self, client, userdata, rc):
        self._log.debug('userdata=%s, rc=%s', userdata, rc)

    def _on_subscribe(self, client, userdata, mid, granted_qos):
        self._log.debug('userdata=%s, mid=%s, granted_qos=%s',
                        userdata, mid, granted_qos)

    def _on_unsubscribe(self, client, userdata, mid):
        self._log.debug('userdata=%s, mid', userdata, mid)

    def _on_message(self, client, userdata, msg):
        self._log.debug('userdata=%s, msg=%s', userdata, msg)

        payload = json.loads(msg.payload.decode('utf-8'))
        self._log.debug('payload=%s', payload)

        data = self.payload2data(payload)
        self._log.debug('data=%s', data)

        ts = self.get_ts(msg, payload)
        self._log.debug('ts=%s', ts)

        if self._cb_func is not None:
            self._cb_func(data, ts)

    def _on_publish(self, client, userdata, mid):
        self._log.debug('userdata=%s, mid=%s', userdata, mid)


class MqttPublisher(MqttClientServer):
    def __init__(self, topic, user='', pw='',
                 host=MqttClientServer.DEF_HOST,
                 port=MqttClientServer.DEF_PORT,
                 debug=False):
        self._dbg = debug
        self._log = get_logger(__class__.__name__, self._dbg)
        self._log.debug('topic=%s', topic)
        self._log.debug('user=%s, pw=%s, host=%s, port=%s',
                        user, pw, host, port)

        super().__init__(None, None, topic, user, pw, host, port, self._dbg)


class MqttSubscriber(MqttClientServer):
    def __init__(self, cb_func, topic, user='', pw='',
                 host=MqttClientServer.DEF_HOST,
                 port=MqttClientServer.DEF_PORT,
                 debug=False):
        self._dbg = debug
        self._log = get_logger(__class__.__name__, self._dbg)
        self._log.debug('topic=%s', topic)
        self._log.debug('user=%s, pw=%s, host=%s, port=%s',
                        user, pw, host, port)

        super().__init__(cb_func, topic, None, user, pw, host, port, self._dbg)


class BeebotteClientServer(MqttClientServer):
    HOST = 'mqtt.beebotte.com'
    PORT = 1883

    def __init__(self, cb_func=None, topic_request=None, topic_reply=None,
                 token='', debug=False):
        self._dbg = debug
        self._log = get_logger(__class__.__name__, self._dbg)
        self._log.debug('topic_request=%s, topic_reply=%s, token=%s',
                        topic_request, topic_reply, token)

        super().__init__(cb_func, topic_request, topic_reply, token, '',
                         self.HOST, self.PORT, debug=self._dbg)

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


class BeebottePublisher(BeebotteClientServer):
    def __init__(self, topic, token='', debug=False):
        self._dbg = debug
        self._log = get_logger(__class__.__name__, self._dbg)
        self._log.debug('topic=%s, token=%s', topic, token)

        super().__init__(None, None, topic, token, self._dbg)


class BeebotteSubscriber(BeebotteClientServer):
    def __init__(self, cb_func, topic, token='', debug=False):
        self._dbg = debug
        self._log = get_logger(__class__.__name__, self._dbg)
        self._log.debug('topic=%s, token=%s', topic, token)

        super().__init__(cb_func, topic, None, token, self._dbg)


class App:
    def __init__(self, topic_request, topic_reply,
                 user, pw='',
                 host=MqttClientServer.DEF_HOST,
                 port=MqttClientServer.DEF_PORT,
                 debug=False):
        self._dbg = debug
        self._log = get_logger(__class__.__name__, self._dbg)

        self.svr = MqttClientServer(self.cb, topic_request, topic_reply,
                                    user, pw, host, port, debug=self._dbg)

    def cb(self, data, ts):
        self._log.info('data=%a ts=%s', data, ts)

        if data == 'exit':
            self.svr.active = False

        self.svr.send_data('>>%s<<' % data)

    def main(self):
        self.svr.start()

        while self.svr.active:
            print('Sleep ..')
            time.sleep(5)

    def end(self):
        self.svr.end()


@click.command(context_settings=CONTEXT_SETTINGS, help='''
MQTT Simple Server App
''')
@click.argument('topic_request', type=str)
@click.argument('topic_reply', type=str)
@click.argument('user', type=str)
@click.argument('password', type=str)
@click.argument('host', type=str)
@click.option('--port', '-p', 'port', type=int,
              default=MqttClientServer.DEF_PORT,
              help='server port')
@click.option('--debug', '-d', 'debug', is_flag=True, default=False,
              help='debug flag')
def main(topic_request, topic_reply, user, password, host, port, debug):
    log = get_logger(__name__, debug=debug)

    app = App(topic_request, topic_reply,
              user, password, host, port,
              debug=debug)
    try:
        app.main()
    finally:
        log.debug('finally')
        app.end()
        log.debug('done')


if __name__ == '__main__':
    main()
