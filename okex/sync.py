#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import time
import queue
import logging
import threading
import pymysql.cursors
from decimal import *
from okex.okexAPI.rest.OkcoinSpotAPI import *
from okex.config import *
from okex.base import *
from okex.ops import *
from logger import *


class SyncPairsThrdLaunch():
    def __init__(self, api_meth, db_meth, **kwargs):
        self.api_meth = api_meth
        self.db_meth = db_meth
        self.pair_queue = queue.Queue()
        self.param_queue = queue.Queue()
        self.lock = threading.Lock()
        self.api_threads = []
        self.db_threads = []
        self.kwargs = kwargs

    def __set_pair_queue(self, data):
        if type(data) is list:
            for raw in data:
                self.pair_queue.put(raw)

    def __init_thread(self):
        for i in range(MAX_THRD_API):
            thread = SyncPairsFetchThrd(
                self.api_meth,
                self.db_meth,
                self.pair_queue,
                self.param_queue,
                self.lock,
                **self.kwargs,
                ID=i)
            thread.setDaemon(True)
            thread.start()
            self.api_threads.append(thread)

        for i in range(MAX_THRD_OPDB):
            thread = SyncPairsInsertThrd(
                self.db_meth, self.param_queue, self.lock, **self.kwargs, ID=i)
            thread.setDaemon(True)
            thread.start()
            self.db_threads.append(thread)

    def start(self):
        parser = OpDbPair(EXCHANGE)
        pairs = parser.get_active()
        parser.close()
        self.__set_pair_queue(pairs)
        self.__init_thread()
        self.pair_queue.join()
        self.param_queue.join()


class SyncPairsFetchThrd(threading.Thread):
    def __init__(self, api_meth, db_meth, pair_queue, param_queue, lock,
                 **kwargs):
        threading.Thread.__init__(self)
        self.api_meth = api_meth
        self.db_meth = db_meth
        self.pair_queue = pair_queue
        self.param_queue = param_queue
        self.lock = lock
        self.kwargs = kwargs
        self.timer = OpTimer(MAX_THRD_IDLE_TIMEOUT)

    def run(self):
        while True:
            with self.lock:
                if not self.pair_queue.empty():
                    self.timer.reset()
                    pair = self.pair_queue.get()
                    db_parser = globals().get(self.db_meth)(**pair,
                                                            **self.kwargs)
                    stamp = db_parser.get_last_stamp()
                    db_parser.close()
                    api_parser = OpRawResponse(self.api_meth, **pair,
                                               **self.kwargs)
                    param = api_parser.get_insert_param(stamp)
                    api_parser.close()
                    if param and len(param) > 1:
                        self.param_queue.put({'pair': pair, 'param': param})
                        param = None
                    self.pair_queue.task_done()
                elif self.timer.timesup():
                    break


class SyncPairsInsertThrd(threading.Thread):
    def __init__(self, db_meth, param_queue, lock, **kwargs):
        threading.Thread.__init__(self)
        self.db_meth = db_meth
        self.param_queue = param_queue
        self.lock = lock
        self.kwargs = kwargs
        self.timer = OpTimer(MAX_THRD_IDLE_TIMEOUT)

    def run(self):
        while True:
            with self.lock:
                if not self.param_queue.empty():
                    self.timer.reset()
                    data = self.param_queue.get()
                    pair = data.get('pair')
                    param = data.get('param')
                    db_parser = globals().get(self.db_meth)(**pair,
                                                            **self.kwargs)
                    affected = db_parser.insert(param, many=True)

                    #-----Info Log-----#
                    global INSERT_COUNT
                    if (affected) is int:
                        INSERT_COUNT += affected
                    logger.info(
                        'Insert Thread {0}: {1} rows for ({2}, {3}, {4}) affected. {5} rows affected in total.'.
                        format(
                            self.kwargs.get('ID'), affected,
                            pair.get('pair_id'), pair.get('symbol'),
                            self.kwargs.get('period'), INSERT_COUNT))
                    #-----Info Log-----#

                    param = None
                    self.param_queue.task_done()

                elif self.timer.timesup():
                    break
