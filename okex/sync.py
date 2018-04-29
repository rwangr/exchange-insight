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
from okex.base import *
from okex.ops import *
from logger import *

MAX_THRD_API = 5
MAX_THRD_OPDB = 5
MAX_THRD_IDLE_TIMEOUT = 2
INSERT_COUNT = 0


class SyncPairsThrdLaunch():
    def __init__(self, api_meth, db_meth, **kwargs):
        self.api_meth = api_meth
        self.db_meth = db_meth
        self.pair_queue = queue.Queue()
        self.data_queue = queue.Queue()
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
                self.data_queue,
                self.lock,
                **self.kwargs,
                ID=i)
            thread.setDaemon(True)
            thread.start()
            self.api_threads.append(thread)

        for i in range(MAX_THRD_OPDB):
            thread = SyncPairsInsertThrd(
                self.db_meth, self.data_queue, self.lock, **self.kwargs, ID=i)
            thread.setDaemon(True)
            thread.start()
            self.db_threads.append(thread)

    def start(self):
        parser = OpDbPair('okex')
        pairs = parser.get_active()
        self.__set_pair_queue(pairs)
        self.__init_thread()
        self.pair_queue.join()
        self.data_queue.join()


class SyncPairsFetchThrd(threading.Thread):
    def __init__(self, api_meth, db_meth, pair_queue, data_queue, lock,
                 **kwargs):
        threading.Thread.__init__(self)
        self.api_meth = api_meth
        self.db_meth = db_meth
        self.pair_queue = pair_queue
        self.data_queue = data_queue
        self.lock = lock
        self.kwargs = kwargs
        self.timer = OpTimer(MAX_THRD_IDLE_TIMEOUT)

    def run(self):
        api_parser = OpRawResponse(self.api_meth, **self.kwargs)
        while True:
            with self.lock:
                if not self.pair_queue.empty():
                    self.timer.reset()
                    data = self.pair_queue.get()
                    db_parser = globals().get(self.db_meth)(**data,
                                                            **self.kwargs)
                    stamp = db_parser.get_last_stamp()
                    del db_parser
                    data = api_parser.get_insert_param(stamp)
                    if data and len(data) > 1:
                        self.data_queue.put(data)
                        data = None
                    self.pair_queue.task_done()
                elif self.timer.timesup():
                    break


class SyncPairsInsertThrd(threading.Thread):
    def __init__(self, db_meth, data_queue, lock, **kwargs):
        threading.Thread.__init__(self)
        self.db_meth = db_meth
        self.data_queue = data_queue
        self.lock = lock
        self.kwargs = kwargs
        self.timer = OpTimer(MAX_THRD_IDLE_TIMEOUT)

    def run(self):
        self.db_parser = globals().get(self.db_meth)(**self.kwargs)
        while True:
            with self.lock:
                if not self.data_queue.empty():
                    self.timer.reset()
                    data = self.data_queue.get()
                    if data:
                        affected = self.db_parser.insert(data, many=True)
                        data = None
                    self.data_queue.task_done()

                    #-----Info Log-----#
                    global INSERT_COUNT
                    if (affected) is int:
                        INSERT_COUNT += affected
                    logger.info(
                        'Insert Thread {0}: {1} rows for ({2}, {3}, {4}) affected. {5} rows affected in total.'.
                        format(
                            self.kwargs.get('ID'), affected,
                            self.kwargs.get('pair_id'),
                            self.kwargs.get('symbol'),
                            self.kwargs.get('period'), INSERT_COUNT))
                    #-----Info Log-----#

                elif self.timer.timesup():
                    break
