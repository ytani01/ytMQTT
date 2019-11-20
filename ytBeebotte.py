#!/usr/bin/env python3
#
# (C) 2019 Yoichi Tanibayashi
#
__author__ = 'Yoichi Tanibayashi'
__date__   = '2019'

from ytMqtt import Mqtt
import time

from MyLogger import get_logger


class Beebotte(Mqtt):
    HOST = 'mqtt.beebotte.com'
    PORT = 1883

    DEF_QOS = 0

    def __init__(self, topic, debug=False):
        self._debug = debug
        self._logger = get_logger(__class__.__name__, self._debug)
        self._logger.debug('topic=%s', topic)

        super().__init__(topic, self.HOST, port=self.PORT, debug=self._debug)

    def publish(self, topic, data, qos=DEF_QOS, retain=False):
        self._logger.debug('topic=%s, data=%s, qos=%d, retain=%s',
                           topic, data, qos, retain)

        payload = self.data2payload(data)
        super().publish(topic, payload, qos=qos, retain=retain)

    def ts2datestr(self, ts_msec):
        self._logger.debug('ts_msec=%d', ts_msec)

        datestr = time.strftime('%Y/%m/%d,%H:%M:%S',
                                time.localtime(ts_msec / 1000))
        return datestr

    def data2payload(self, data):
        self._logger.debug('data=%s', data)

        ts = int(time.time() * 1000)
        payload = {'data': data, 'ts': ts, 'ispublic': False}
        self._logger.debug('payload=%s', payload)
        return payload


class App:
    def __init__(self, topic, debug=False):
        self._debug = debug
        self._logger = get_logger(__class__.__name__, self._debug)
        self._logger.debug('topic=%s', topic)

        self._topic = topic

        self._bbt = Beebotte(self._topic, debug=self._debug)
        self._bbt.start()

    def main(self):
        self._logger.debug('')

        self._bbt.subscribe()
        for t in self._topic:
            self._bbt.publish(t, 'hello')

        for i in range(10):
            msg_type, msg_data = self._bbt.get_msg()
            if msg_type == Beebotte.MSG_DATA:
                topic = msg_data['topic']
                payload = msg_data['payload']

                data = payload['data']
                datestr = self._bbt.ts2datestr(payload['ts'])

                print('(%d) %s %s: %s' % (i, datestr, topic, data))
            else:
                print('(%d) %s' % (i, msg_data))
            time.sleep(2)

    def end(self):
        self._logger.debug('')
        self._bbt.end()


class AppServer:
    def __init__(self, topic, debug=False):
        self._debug = debug
        self._logger = get_logger(__class__.__name__, self._debug)
        self._logger.debug('topic=%s')

        super().__init__(topic, Beebotte.HOST, Beebotte.PORT,
                         debug=self._debug)

    def main(self):
        self._logger.debug('')

        while self._loop:
            payload = self.recv_request()
            self._logger.debug('payload=%s', payload)

            data = payload['data']
            print('data=%s' % data)

            time.sleep(2)

            self.send_reply(data)

    def recv_request(self):
        self._logger.debug('')

        payload = super().recv_request()
        return payload


class AppClient:
    def __init__(self, topic, debug=False):
        self._debug = debug
        self._logger = get_logger(__class__.__name__, self._debug)
        self._logger.debug('topic=%s')

        super().__init__(topic, Beebotte.HOST, Beebotte.PORT,
                         debug=self._debug)

    def main(self):
        self._logger.debug('')

        while self._loop:
            data = input()
            self._logger.debug('data=%s', data)

            self.send_request(data)

            payload = self.recv_reply()
            self._logger.debug('payload=%s', payload)
            print(payload['data'])

    def end(self):
        self._logger.debug('')

        self._logger.debug('done')

    def send_request(self, data):
        self._logger.debug('data=%s', data)
        self._bbt.publish(self._t_request, data)


import click
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS,
               help='''
beebotte MQTT class
''')
@click.argument('topic1')
@click.argument('topic2', nargs=-1)
@click.option('--mode', '-m', 'mode', type=str, default='',
              help='mode: \'\', \'s\', \'c\'')
@click.option('--debug', '-d', 'debug', is_flag=True, default=False,
              help='debug flag')
def main(topic1, topic2, mode, debug):
    logger = get_logger(__name__, debug=debug)
    logger.debug('topic1=%s, topic2=%s, mode=%s', topic1, topic2, mode)

    topic = [topic1] + list(topic2)

    if mode == '':
        app = App(topic, debug=debug)
    if mode == 's':
        app = AppServer(topic, debug=debug)
    if mode == 'c':
        app = AppClient(topic, debug=debug)

    try:
        app.main()
    finally:
        logger.info('finally')
        app.end()


if __name__ == '__main__':
    main()
