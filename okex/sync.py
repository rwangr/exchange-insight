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

MAX_THRD_FULL = 3
MAX_THRD_ONE = 2
MAX_THRD_IDLE_TIMEOUT = 2
INSERT_COUNT = 0


class SyncOnePairThrd(threading.Thread):
    def __init__(self, db_meth, queue, lock, **kwargs):
        threading.Thread.__init__(self)
        self.db_meth = db_meth
        self.queue = queue
        self.lock = lock
        self.kwargs = kwargs
        self.timer = OpTimer(MAX_THRD_IDLE_TIMEOUT)

    def run(self):
        self.db_parser = globals().get(self.db_meth)(**self.kwargs)
        while True:
            with self.lock:
                if not self.queue.empty():
                    data = self.queue.get()
                    affected = self.db_parser.insert(data, many=True)
                    self.queue.task_done()
                    self.timer.reset()

                    #-----Info Log-----#
                    global INSERT_COUNT
                    if (affected) is int:
                        INSERT_COUNT += affected
                    logger.info(
                        'Thread {0}-{1}: {2} rows for ({3}, {4}, {5}) affected. {6} rows affected in total.'.
                        format(
                            self.kwargs.get('ID_1'), self.kwargs.get('ID_2'),
                            affected, self.kwargs.get('pair_id'),
                            self.kwargs.get('symbol'),
                            self.kwargs.get('period'), INSERT_COUNT))
                    #-----Info Log-----#

                elif self.timer.timesup():
                    break


class SyncOnePairThrdLaunch():
    def __init__(self, api_meth, db_meth, **kwargs):
        self.api_meth = api_meth
        self.db_meth = db_meth
        self.kwargs = kwargs
        self.queue = queue.Queue()
        self.lock = threading.Lock()
        self.threads = []

    def __set_queue(self, data):
        self.queue.put(data)

    def __init_thread(self):
        for i in range(MAX_THRD_ONE):
            thread = SyncOnePairThrd(
                self.db_meth, self.queue, self.lock, **self.kwargs, ID_2=i)
            thread.setDaemon(True)
            thread.start()
            self.threads.append(thread)

    def start(self):
        self.__init_thread()
        api_parser = OpRawResponse(self.api_meth, **self.kwargs)
        db_parser = globals().get(self.db_meth)(**self.kwargs)
        while True:
            stamp = db_parser.get_last_stamp()
            data = api_parser.get_insert_param(stamp)
            self.__set_queue(data)
            self.queue.join()
            if len(data) <= 1:
                break
        for t in self.threads:
            t.join()

    def __del__(self):
        del self


class SyncFullPairThrd(threading.Thread):
    def __init__(self, api_meth, db_meth, queue, lock, **kwargs):
        threading.Thread.__init__(self)
        self.api_meth = api_meth
        self.db_meth = db_meth
        self.queue = queue
        self.lock = lock
        self.kwargs = kwargs
        self.timer = OpTimer(MAX_THRD_IDLE_TIMEOUT)

    def run(self):
        while True:
            with self.lock:
                if not self.queue.empty():
                    data = self.queue.get()
                    self.timer.reset()
                else:
                    data = None
                    if self.timer.timesup():
                        break
            if data:
                parser = SyncOnePairThrdLaunch(self.api_meth, self.db_meth,
                                               **data, **self.kwargs)
                parser.start()
                data = None
                del parser
                self.queue.task_done()


class SyncFullPairThrdLaunch():
    def __init__(self, api_meth, db_meth, **kwargs):
        self.api_meth = api_meth
        self.db_meth = db_meth
        self.kwargs = kwargs
        self.queue = queue.Queue()
        self.lock = threading.Lock()
        self.threads = []

    def __set_queue(self, data):
        if type(data) is list:
            for raw in data:
                self.queue.put(raw)

    def __init_thread(self):
        for i in range(MAX_THRD_FULL):
            thread = SyncFullPairThrd(
                self.api_meth,
                self.db_meth,
                self.queue,
                self.lock,
                **self.kwargs,
                ID_1=i)
            thread.setDaemon(True)
            thread.start()
            self.threads.append(thread)

    def start(self):
        self.__init_thread()
        parser = OpDbPair('okex')
        pairs = parser.get_active()
        self.__set_queue(pairs)
        self.queue.join()
        #for t in self.threads:
        #    t.join()
