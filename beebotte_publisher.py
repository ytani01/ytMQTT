#!/usr/bin/env python3
#
# sample publisher for Beebotte
#
from Mqtt import BeebottePublisher as BBT
import click


@click.command(help='Beebotte publisher')
@click.argument('token', type=str)
@click.argument('topic', type=str)
@click.option('--debug', '-d', 'debug', is_flag=True, default=False,
              help='debug flag')
def main(token, topic, debug):
    bbt = BBT(token, debug=debug)
    bbt.start()
    while True:
        data = input('>> Ready <<\n')
        bbt.send_data(data, topic)


if __name__ == '__main__':
    main()
