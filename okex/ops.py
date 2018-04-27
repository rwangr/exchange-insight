#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import time
import logging
import pymysql.cursors
from decimal import *
from okex.okexAPI.rest.OkcoinSpotAPI import *
from okex.base import *
from logger import *


class OpRawBase():
    def __init__(self, **kwargs):
        self.handler = APIConnBase().connect()


class OpRawResponse(OpRawBase):
    def __init__(self, api_meth, **kwargs):
        self.api_meth = api_meth
        self.kwargs = kwargs
        OpRawBase.__init__(self, **self.kwargs)

    def __list_to_tuple(self, data, sort_key=[], head=[]):
        result = []
        if type(data) == list:
            for raw in data:
                if type(raw) == list:
                    if sort_key:
                        result.append(
                            tuple(head + [raw[key] for key in sort_key]))
                    else:
                        result.append(tuple(head + raw))
                elif type(raw) == dict:
                    if sort_key:
                        result.append(
                            tuple(head + [raw.get(key) for key in sort_key]))
                    else:
                        result.append(tuple(head + list(raw.values())))
        return tuple(result)

    def get_insert_param(self, stamp):
        data = eval('self.handler.%s' % self.api_meth)(
            **self.kwargs, since=stamp)
        if type(data) == dict and data.get('error_code'):
            #-----Warning Log-----#
            logger.warning(
                'API {0} Responds Error: code {1}, Param(kwargs: {2},stamp: {3})'.
                format(self.api_meth, data.get('error_code'), self.kwargs,
                       stamp))
            #-----Warning Log-----#
            return None
        if self.api_meth == 'kline':
            sort_key = None
            head = [self.kwargs.get('pair_id'), self.kwargs.get('period')]
        elif self.api_meth == 'trades':
            sort_key = ['tid', 'date', 'date_ms', 'price', 'amount', 'type']
            head = [self.kwargs.get('pair_id')]
        param = self.__list_to_tuple(data, sort_key, head)
        return param


class OpDbBase():
    def __init__(self, **kwargs):
        self.executor = SQLExecutor(**kwargs)


class OpDbCandlestick(OpDbBase):
    def __init__(self, pair_id, period, **kwargs):
        self.pair_id = pair_id
        self.period = period
        self.kwargs = kwargs
        OpDbBase.__init__(self, **kwargs)

    def insert(self, param, **kwargs):
        sql = "INSERT IGNORE INTO `OKEX_CANDLESTICK_QA_{0}` \
            (`PAIR_ID`, `PERIOD`, `TIMESTAMP`, `OPEN`, `CLOSE`, `HIGH`, `LOW`, `VOLUME`) \
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)".format(
            self.period.upper())

        affected = self.executor.set(sql, param, **kwargs)
        return affected

    def get_last_stamp(self):
        sql = "SELECT `SEQ`,`TIMESTAMP` FROM `OKEX_CANDLESTICK_QA_{0}` \
             WHERE `PAIR_ID`=%s \
             ORDER BY `SEQ` DESC LIMIT 1".format(self.period.upper())

        result = self.executor.get(sql, param=(self.pair_id))
        if result != ['ERROR']:
            return result.get('TIMESTAMP') if result else '0'
        else:
            return '99999999999999'


class OpDbTransaction(OpDbBase):
    def __init__(self, pair_id, **kwargs):
        self.pair_id = pair_id
        self.kwargs = kwargs
        OpDbBase.__init__(self, **kwargs)

    def insert(self, param, **kwargs):
        sql = "INSERT IGNORE INTO `OKEX_TRANSACTION_QA` \
            (`PAIR_ID`, `TID`, `DATE`, `DATE_MS`, `PRICE`, `AMOUNT`, `TYPE`) \
            VALUES (%s, %s, %s, %s, %s, %s, %s)"

        affected = self.executor.set(sql, param, **kwargs)
        return affected

    def get_last_stamp(self):
        sql = "SELECT `SEQ`,`TID` FROM `OKEX_TRANSACTION_QA` \
             WHERE `PAIR_ID`=%s \
             ORDER BY `seq` DESC LIMIT 1"

        result = self.executor.get(sql, param=(self.pair_id))
        if result != ['ERROR']:
            return result.get('TID') if result else '0'
        else:
            return '99999999999999'


class OpDbPair(OpDbBase):
    def __init__(self, exchange, **kwargs):
        self.exchange = exchange
        self.quotes = ['btc', 'eth', 'usdt']
        self.kwargs = kwargs
        OpDbBase.__init__(self, **kwargs)

    def insert(self, base):
        sql = "INSERT IGNORE INTO `GLOBAL_PAIRS` \
            (`BASE`, `QUOTE`, `EXCHANGE`) \
            VALUES (%s, %s, %s)"

        param = []
        for quote in self.quotes:
            if base != quote and base not in self.quotes:
                param.append((base, quote, self.exchange))
        param = tuple(param)

        affected = self.executor.set(sql, param, many=True)
        return affected

    def get_active(self):
        sql = "SELECT `PAIR_ID` AS 'pair_id', CONCAT(`BASE`,'_',`QUOTE`) AS 'symbol' \
            FROM `GLOBAL_PAIRS` WHERE `EXCHANGE`=%s AND `ACTIVE`='1' \
            ORDER BY `PAIR_ID`"

        result = self.executor.get(sql, param=(self.exchange), fetchall=True)
        if result and result != ['ERROR']:
            return result
