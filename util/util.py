# -*- coding: utf-8 -*-
"""
Created on 2017/3/2 23:23

@version: python3.5
@author: qd
"""

import datetime
import my_path.path
import util.const as const
# import const


def time_int_to_datetime(time_int, date):
    # assume int type is like 104212000
    if isinstance(date, str):
        year = int(date[0:4])
        month = int(date[4:6])
        day = int(date[6:8])
    else:
        year = date.year
        month = date.month
        day = date.day
    time_str = str(time_int)[:-3]
    hour = int(time_str[:-4])
    minute = int(time_str[-4:-2])
    second = int(time_str[-2:])
    date_datetime = datetime.datetime(year, month, day, hour, minute, second)
    return date_datetime


def in_market_open_time(date_datetime):
    date_ = datetime.datetime(1900, 1, 1, date_datetime.hour, date_datetime.minute, date_datetime.second)
    if (const.MARKET_END_TIME_MORNING >= date_ >= const.MARKET_OPEN_TIME_MORNING) or (const.MARKET_END_TIME_AFTERNOON >= date_ >= const.MARKET_OPEN_TIME_AFTERNOON):
        return True
    else:
        return False


def merge_date_and_time(date_datetime, time_datetime):
    n = datetime.datetime(date_datetime.year, date_datetime.month, date_datetime.day, time_datetime.hour, time_datetime.minute, time_datetime.second)
    return n


def get_lagged_time(time_now_datetime, lag, yesterday_datetime):
    # if lag == 0:
    #     time = data['nTime'].max()
    # else:
    #     time_now = data['nTime'].max()
    #     time = time_now - datetime.timedelta(minutes=lag)

    time_tmp = time_now_datetime - datetime.timedelta(minutes=lag)
    if time_tmp.hour < 9 or (time_tmp.hour == 9 and time_tmp.minute < 30):
        time_delta = merge_date_and_time(time_now_datetime, const.MARKET_OPEN_TIME_MORNING) - time_tmp
        time_new = merge_date_and_time(yesterday_datetime, const.MARKET_END_TIME_AFTERNOON) - time_delta

    elif time_tmp <= merge_date_and_time(time_now_datetime, const.MARKET_OPEN_TIME_AFTERNOON) and time_now_datetime >= merge_date_and_time(time_now_datetime, const.MARKET_OPEN_TIME_AFTERNOON):
        time_delta = merge_date_and_time(time_now_datetime, const.MARKET_OPEN_TIME_AFTERNOON) - time_tmp
        time_new = merge_date_and_time(time_now_datetime, const.MARKET_END_TIME_MORNING) - time_delta

    else:
        time_new = time_tmp

    return time_new


def get_venue(stk_code):
    if stk_code == '000300':
        return 'SH'

    if any(map(stk_code.startswith, ['000', '001', '002', '300'])):
        return 'SZ'
    else:
        return 'SH'


def isvalid_stk_code(code_str):
    if any(map(code_str.startswith, ['000', '001', '002', '300', '600', '601', '603'])):
        return True
    else:
        return False


def datetime2hms(time):
    return time.strftime('%H:%M:%S')


if __name__ == '__main__':
    time_now = datetime.datetime(2017,3,6,14,40)
    lag = 50
    yesterday_datetime = datetime.datetime(2017,3,4)
    print(get_lagged_time(time_now, lag, yesterday_datetime))


