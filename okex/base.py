#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import time
import logging
import pymysql.cursors
from DBUtils.PooledDB import PooledDB
from DBUtils.PersistentDB import PersistentDB
from okex.okexAPI.rest.OkcoinSpotAPI import *
from decimal import *
from okex.config import *
from okex.secret import *
from logger import *


class DBConnBase():
    def __init__(self, **kwargs):
        global CONN_POOL
        try:
            CONN_POOL
        except NameError:
            # CONN_POOL = PersistentDB(
            #     creator=pymysql,
            #     maxusage=5000,
            #     ping=1,
            #     closeable=True,
            #     host=CONN_STRING['host'],
            #     port=CONN_STRING['port'],
            #     user=CONN_STRING['user'],
            #     password=CONN_STRING['password'],
            #     db=CONN_STRING['db'],
            #     charset=CONN_STRING['charset'])
            CONN_POOL = PooledDB(
                creator=pymysql,
                maxconnections=CONN_POOL_MAX_CONNECTIONS,
                mincached=CONN_POOL_MIN_CACHED,
                maxcached=CONN_POOL_MAX_CACHED,
                maxshared=CONN_POOL_MAX_SHARED,
                maxusage=CONN_POOL_MAX_USAGE,
                blocking=True,
                setsession=[],
                ping=0,
                host=CONN_STRING['host'],
                port=CONN_STRING['port'],
                user=CONN_STRING['user'],
                password=CONN_STRING['password'],
                db=CONN_STRING['db'],
                charset=CONN_STRING['charset'])

        else:
            pass

    def connect(self):
        return CONN_POOL.connection()

    def __del__(self):
        pass


class SQLExecutor():
    def __init__(self, **kwargs):
        db_parser = DBConnBase(**kwargs)
        self.connection = db_parser.connect()

        #-----Debug Log-----#
        global CONN_COUNT
        CONN_COUNT += 1
        logger.debug('Thread {0}: {1} connections in total. {2}.'.format(
            kwargs.get('ID'), CONN_COUNT, kwargs))
        #-----Debug Log-----#

    def set(self, sql, param=(), many=False):
        try:
            cursor = self.connection.cursor()
            if many:
                result = cursor.executemany(sql, param)
            else:
                result = cursor.execute(sql, param)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()

            #-----Warning Log-----#
            logger.warning('SQLExcutor set() Error: {0} : ({1})'.format(
                str(e), param))
            #-----Warning Log-----#

        finally:
            cursor.close()
            return result

    def get(self, sql, param=(), fetchall=False):
        try:
            cursor = self.connection.cursor(cursor=pymysql.cursors.DictCursor)
            cursor.execute(sql, param)
            self.connection.commit()
            if fetchall:
                result = cursor.fetchall()
            else:
                result = cursor.fetchone()
        except Exception as e:
            result = ['ERROR']

            #-----Warning Log-----#
            logger.warning('SQLExcutor get() Error: {0} : ({1})'.format(
                str(e), param))
            #-----Warning Log-----#

        finally:
            cursor.close()
            return result

    def __del__(self):
        self.connection.close()
        global CONN_COUNT
        CONN_COUNT -= 1

    # import importlib
    # module = importlib.import_module(method)
    # buy_res = module.func(params)


class APIConnBase:
    def __init__(self, api_clas='OKCoinSpot', **kwargs):
        self.api_clas = api_clas

    def connect(self):
        parser = globals().get(self.api_clas)(API_STRING['okcoinRESTURL'],
                                              API_STRING['apikey'],
                                              API_STRING['secretkey'])

        return parser

