# -*- coding: utf-8 -*-
"""
Created on 2017/3/11 12:44

@version: python3.5
@author: qd
"""

import sql.sql
import my_path.path
import log.log
import util.util

import pandas as pd
import os


this_log = log.log.DataDownload()


def get_daily_ret_me_from_csv(beg_date, end_date):
    this_log.info('start get dailyretme from csv')

    data = pd.read_csv(my_path.path.daily_ret_me_csv_path)

    this_log.info('got dailyretme from csv {}'.format(len(data)))

    beg_date_reformat = '{}-{}-{}'.format(beg_date[0:4], beg_date[4:6], beg_date[6:])
    end_date_reformat = '{}-{}-{}'.format(end_date[0:4], end_date[4:6], end_date[6:])

    idx = (data['date'] <= end_date_reformat) & (data['date'] >= beg_date_reformat)
    data_selected_date = data[idx]

    data_selected_date.loc[:, 'id'] = data_selected_date['id'].apply(lambda x: str(x).zfill(6))

    idx_valid_stk = data_selected_date['id'].apply(util.util.isvalid_stk_code)

    data_selected = data_selected_date[idx_valid_stk]
    this_log.info('end get dailyretme from csv {}'.format(len(data_selected)))
    return data_selected


def get_data_path(date, stk, venue):
    path_ = my_path.path.one_min_bid_ask_path
    path__ = '{}{}\\{}\\{}.csv'.format(path_, venue, date, stk)
    return path__


def check_data(date, stk, venue):
    path_ = get_data_path(date, stk, venue)

    try:
        data = pd.read_csv(path_)
        if len (data) >= 100 and len(data.columns) >= 5:
            return True, None
        else:
            return False, 'len not match'
    except Exception as e:
        this_log.info('{}, {}'.format(path_, e))
        return False, str(e)


def download_one_day_one_stk(path_out, venue, date, stk):
    path_out_root = path_out + venue.upper() + '\\' + date
    path_out_file = path_out + venue.upper() + '\\' + date + '\\' + stk + '.csv'

    if os.path.exists(path_out_file):
        this_log.info('file exists: ' + path_out_file)
        return True

    this_log.info('begin: ' + path_out_file)

    try:
        data_in = sql.sql.get_hf_data(date, stk, 'tick', venue)

        time_idx_not_0 = data_in['time'] != 0
        data_in = data_in[time_idx_not_0]

        data_in['time'] = pd.to_datetime(data_in['time'].astype(str), format='%H%M%S%f')
        data_in = data_in.drop_duplicates('time', keep='last')
        data_in_ = data_in.set_index('time')
        data_out = data_in_.resample('1min', label='right', closed='left').apply('last')
        data_out_ = data_out.reset_index()
        data_out_['time'] = data_out_['time'].apply(util.util.datetime2hms)
        data_out_.fillna(method='ffill', inplace=True)

        if not os.path.exists(path_out_root):
            os.makedirs(path_out_root)
        data_out_.to_csv(path_out_file, index=None)

        this_log.info('done: ' + path_out_file)
        return True
    except Exception as e:
        this_log.error('error: {}, {}, {}, {}, {}'.format(path_out, venue, date, stk, e))
        return False
