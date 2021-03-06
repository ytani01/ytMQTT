#!/usr/bin/env python3
#
# (c) 2019 Yoichi Tanibayashi
#
"""
MyLogger.py

Usage:
------
from MyLogger import get_logger

class A:
    _log = get_logger(__name__, False)

    def __init__(self, a, debug=False)
        self._dbg = debug
        __class__._log = get_logger(__class__.__name__, self._dbg)
        self._log.debug('a=%s', a)

    def func1(self, param1):
        self._log.debug('param1=%s', param1)

    @classmethod
    def func2(cls, param1):
        cls._log.debug('param1=%s', param1)    
------

"""
__author__ = 'Yoichi Tanibayashi'
__date__   = '2019'

from logging import getLogger, StreamHandler, Formatter, DEBUG, INFO, WARN


class MyLogger:
    def __init__(self, name=''):
        fmt_hdr = '%(asctime)s %(levelname)s '
        fmt_loc = '%(filename)s.%(name)s.%(funcName)s:%(lineno)d> '
        self.handler_fmt = Formatter(fmt_hdr + fmt_loc + '%(message)s',
                                     datefmt='%H:%M:%S')

        self.console_handler = StreamHandler()
        self.console_handler.setLevel(DEBUG)
        self.console_handler.setFormatter(self.handler_fmt)

        self._log = getLogger(name)
        self._log.setLevel(INFO)
        self._log.addHandler(self.console_handler)
        self._log.propagate = False

    def get_logger(self, name, debug):
        logger = self._log.getChild(name)
        if debug:
            logger.setLevel(DEBUG)
        else:
            logger.setLevel(INFO)
        return logger


myLogger = MyLogger()


def get_logger(name, debug):
    return myLogger.get_logger(name, debug)
