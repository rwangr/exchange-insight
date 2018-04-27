#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import time
import queue
import logging
import argparse
import threading
import pymysql.cursors
from decimal import *
from okex.okexAPI.rest.OkcoinSpotAPI import *
from okex.sync import *
from logger import *


def main(*args, **kwargs):

    # parser = argparse.ArgumentParser(
    #     description='Exchange Insight Data Sync Engine.')
    # parser.add_argument('--api', type=str, default=None)
    # parser.add_argument('--opdb', type=str, default=None)
    # parser.add_argument('--period', type=str, default=None)
    # args = parser.parse_args()

    # parser = SyncFullPairThrdLaunch(
    #     api_meth=args.api, db_meth=args.opdb, period=args.period)
    # parser.start()

    parser = SyncFullPairThrdLaunch(
        api_meth='kline', db_meth='OpDbCandlestick', period='30min')
    parser.start()
    #parser = SyncFullPairThrdLaunch('trades', 'OpDbTransaction')
    #parser.start()


if __name__ == "__main__":
    main()
    sys.exit()