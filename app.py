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
from okex.config import *
from okex.sync import *
from logger import *


def main(*args, **kwargs):

    parser = SyncPairsThrdLaunch(**kwargs)
    parser.start()

    # parser = SyncFullPairThrdLaunch(
    #     api_meth='kline', db_meth='OpDbCandlestick', period='30min')
    # parser.start()

    # parser = SyncFullPairThrdLaunch(api_meth='trades', db_meth='OpDbTransaction')
    # parser.start()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Exchange Insight Data Sync Engine.')
    parser.add_argument('--cwd', type=str, default=os.getcwd())
    parser.add_argument('--api', type=str, default=None)
    parser.add_argument('--opdb', type=str, default=None)
    parser.add_argument('--pd', type=str, default=None)
    args = parser.parse_args()
    os.chdir(str(args.cwd))

    main(api_meth=str(args.api), db_meth=str(args.opdb), period=str(args.pd))
    sys.exit()
