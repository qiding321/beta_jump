# -*- coding: utf-8 -*-
"""
Created on 2017/2/23 18:20

@version: python3.5
@author: qd
"""

import datetime
import os
import socket

name = socket.gethostname()
log_path_root = '..\\log\\'
if not os.path.exists(log_path_root):
    os.makedirs(log_path_root)


if name == '2013-20151201LG':

    daily_ret_me_csv_path = 'I:\\beta_jump_data\\dailyretme.csv'

    one_min_bid_ask_path = 'H:\\OneMinuteBidAsk\\'

    discontinuous_beta_root = 'I:\\beta_jump_data\\beta_discontinuous\\'
    continuous_beta_root = 'I:\\beta_jump_data\\beta_continuous\\'
    overnight_beta_root = 'I:\\beta_jump_data\\beta_overnight\\'
    beta_jump_data_path_root = 'I:\\beta_jump_data\\'

    data_download_record_path = 'H:\\OneMinuteBidAsk2013\\data_download_record\\'

else:
    beta_jump_data_path_root = '\\\\2013-20151201LG\\beta_jump_data\\'
    daily_ret_me_csv_path = '\\\\2013-20151201LG\\beta_jump_data\\dailyretme.csv'

    one_min_bid_ask_path = '\\\\2013-20151201LG\\OneMinuteBidAsk2013\\'

    discontinuous_beta_root = '\\\\2013-20151201LG\\beta_jump_data\\beta_discontinuous\\'
    continuous_beta_root = '\\\\2013-20151201LG\\beta_jump_data\\beta_continuous\\'
    overnight_beta_root = '\\\\2013-20151201LG\\beta_jump_data\\beta_overnight\\'
    data_download_record_path = '\\\\2013-20151201LG\\OneMinuteBidAsk2013\\data_download_record\\'
