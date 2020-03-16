#!/usr/bin/env python3
#
# sample subscriber for Beebotte
#
from Mqtt import BeebotteSubscriber as BBT
import time
import click


def cb(data, topic, ts):
    print('%s[%s] %s' % (BBT.ts2datestr(ts), topic, data))


@click.command(help='Beebotte subscriber')
@click.argument('token', type=str)
@click.argument('topic1', type=str)
@click.argument('topic2', type=str, nargs=-1)
@click.option('--debug', '-d', 'debug', is_flag=True, default=False,
              help='debug flag')
def main(token, topic1, topic2, debug):
    bbt = BBT(cb, [topic1] + list(topic2), token, debug=debug)
    bbt.start()
    while True:
        time.sleep(1)


if __name__ == '__main__':
    main()
