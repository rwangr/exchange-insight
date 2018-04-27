import os
import sys
import time
import logging


class Logger():
    def __init__(self, name):
        self.name = name
        self.time = time.strftime('%H:%M', time.localtime(time.time()))

    def start(self):
        logger = logging.getLogger(self.name)
        logger.setLevel(logging.INFO)
        date = time.strftime('%Y%m%d', time.localtime(time.time()))
        log_path = os.path.split(os.path.realpath(sys.argv[0]))[0] + '/logs/'
        log_filename = log_path + self.name + '_' + date + '.log'
        handler = logging.FileHandler(log_filename, mode='a')
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "{0} | %(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s".
            format(self.time))
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger


logger = Logger('sync').start()
