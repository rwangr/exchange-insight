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
from okex.secret import *
from logger import *

CONN_COUNT = 0


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
                maxconnections=6,
                mincached=2,
                maxcached=6,
                maxshared=0,
                blocking=True,
                maxusage=0,
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
        self.connection = DBConnBase().connect()

        #-----Debug Log-----#
        global CONN_COUNT
        CONN_COUNT += 1
        logger.debug('Thread {0}-{1}: {2} connections in total. {3}'.format(
            kwargs.get('ID_1'), kwargs.get('ID_2'), CONN_COUNT, kwargs))
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
        try:
            self.connection.close()
            global CONN_COUNT
            CONN_COUNT -= 1
        except Exception as e:
            pass

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
