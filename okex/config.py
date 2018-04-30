#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import time
import logging

# Script Setup Params
EXCHANGE = 'okex'
MAX_REMOVE_ERROR_COUNT = 2
MAX_THRD_API = 15
MAX_THRD_OPDB = 5
MAX_THRD_IDLE_TIMEOUT = 2
MIN_UPDATE_THRESHOLD = 1

# Connection Pool Setup Params
CONN_POOL_MAX_CONNECTIONS = 20
CONN_POOL_MIN_CACHED = 15
CONN_POOL_MAX_CACHED = 20
CONN_POOL_MAX_SHARED = 0
CONN_POOL_MAX_USAGE = 0

# Count Values
INSERT_COUNT = 0
CONN_COUNT = 0