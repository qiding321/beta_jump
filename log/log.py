# -*- coding: utf-8 -*-
"""
Created on 2016/12/12 17:28

@version: python3.5
@author: qd
"""

import logging
import os

import my_path.path


class Logger(logging.Logger):
    def __init__(self, name='log', level=logging.DEBUG, add_info=''):
        self.level = level
        logging.Logger.__init__(self, name, level)
        self.log_path = my_path.path.log_path_root + name + '.log'
        file_log = logging.FileHandler(self.log_path)

        self.formatter = logging.Formatter('%(asctime)s' + '%10s' % add_info + ' %(levelname)s %(message)s')

        file_log.setFormatter(self.formatter)
        self.addHandler(file_log)

        console = logging.StreamHandler()
        console.setLevel(self.level)
        console.setFormatter(self.formatter)
        self.addHandler(console)

    def add_path(self, log_path2, level=None):
        log_path2_root, file_name_ = os.path.split(log_path2)
        if not os.path.exists(log_path2_root):
            os.makedirs(log_path2_root)
        file_log2 = logging.FileHandler(log_path2)
        if level is None:
            level = self.level
        file_log2.setLevel(level)
        file_log2.setFormatter(self.formatter)
        self.addHandler(file_log2)

    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR


class LoggerFramework(Logger):
    def __init__(self):
        super(LoggerFramework, self).__init__(name='framework')


class LoggerSQL(Logger):
    def __init__(self):
        super(LoggerSQL, self).__init__(name='framework')


class LoggerBetaJump(Logger):
    def __init__(self):
        super(LoggerBetaJump, self).__init__(name='beta_jump')

class DataDownload(Logger):
    def __init__(self):
        super(DataDownload, self).__init__(name='data_download')
