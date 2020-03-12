#!/usr/bin/env python3
#
# (C) 2019 Yoichi Tanibayashi
#
__author__ = 'Yoichi Tanibayashi'
__date__   = '2019'

from ytMqtt import Mqtt, MqttApp, MqttServerApp, MqttClientApp
import time

from MyLogger import get_logger


class Beebotte(Mqtt):
    HOST = 'mqtt.beebotte.com'
    PORT = 1883

    DEF_QOS = 0

    def __init__(self, topic, user, debug=False):
        self._debug = debug
        self._logger = get_logger(__class__.__name__, self._debug)
        self._logger.debug('topic=%s, user=%s', topic, user)

        super().__init__(topic, user, self.HOST, port=self.PORT,
                         debug=self._debug)

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


import click
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS,
               help='''
beebotte MQTT class
''')
@click.argument('user')
@click.argument('topic1')
@click.argument('topic2', nargs=-1)
@click.option('--mode', '-m', 'mode', type=str, default='',
              help='mode: \'\', \'s\', \'c\'')
@click.option('--debug', '-d', 'debug', is_flag=True, default=False,
              help='debug flag')
def main(user, topic1, topic2, mode, debug):
    logger = get_logger(__name__, debug=debug)
    logger.debug('user=%s, topic1=%s, topic2=%s, mode=%s',
                 user, topic1, topic2, mode)

    topic = [topic1] + list(topic2)

    bbt = Beebotte(topic, user, debug=debug)
    app = None

    if mode == '':
        app = MqttApp(bbt, debug=debug)

    if mode in 'sc':
        if len(topic) != 2:
            print('topic mast be [{request topic}, {reply topic}]')
            return

    if mode == 's':
        app = MqttServerApp(bbt, debug=debug)
    if mode == 'c':
        app = MqttClientApp(bbt, debug=debug)

    if app is None:
        return

    try:
        app.main()
    finally:
        logger.info('finally')
        app.end()


if __name__ == '__main__':
    main()
