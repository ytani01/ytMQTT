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

        super().__init__(self.HOST, topic, port=self.PORT, debug=self._debug)

    def publish(self, topic, data, qos=DEF_QOS, retain=False):
        self._logger.debug('topic=%s, data=%s, qos=%d, retain=%s',
                           topic, data, qos, retain)

        ts = int(time.time() * 1000)
        payload = {'data': data, 'ts': ts, 'ispublic': True}
        self._logger.debug('ts=%d, payload=%s', ts, payload)

        super().publish(topic, payload, qos=qos, retain=retain)

    def ts2datestr(self, ts_msec):
        self._logger.debug('ts_msec=%d', ts_msec)

        datestr = time.strftime('%Y/%m/%d,%H:%M:%S',
                                time.localtime(ts_msec / 1000))
        return datestr


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
            msg_type, msg_data = self._bbt.msg_get()
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


import click
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS,
               help='''
beebotte MQTT class
''')
@click.argument('topic1')
@click.argument('topic2', nargs=-1)
@click.option('--debug', '-d', 'debug', is_flag=True, default=False,
              help='debug flag')
def main(topic1, topic2, debug):
    logger = get_logger(__name__, debug=debug)

    topic = [topic1] + list(topic2)

    app = App(topic, debug=debug)
    try:
        app.main()
    finally:
        logger.info('finally')
        app.end()


if __name__ == '__main__':
    main()
