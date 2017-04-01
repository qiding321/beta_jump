# -*- coding: utf-8 -*-
"""
Created on 2017/3/11 9:55

@version: python3.5
@author: qd
"""

import sql.sql
import my_path.path
import log.log
import util.util
import util.common_data

import pandas as pd
import os
import multiprocessing

#
# def main():
#     path_to_check = my_path.path.one_min_bid_ask_path
#
#     date_beg = '20160101'
#     date_end = '20161231'
#     this_log.info('date_begin: {}, date_end: {}'.format(date_beg, date_end))
#
#     # daily_ret_me_data = sql.sql.get_daily_ret_me(date_beg, date_end)
#     daily_ret_me_data = util.common_data.get_daily_ret_me_from_csv(date_beg, date_end)
#
#     daily_ret_me_data = daily_ret_me_data.sort_values(by=['date', 'id'])
#     daily_ret_me_data.groupby('date').count()
#
#     date_stk_pairs = zip(daily_ret_me_data['date'].tolist(), daily_ret_me_data['id'].tolist())
#
#     date_now = None
#
#     for date_, stk in date_stk_pairs:
#
#         if date_ != date_now:
#             this_log.info('{} begin'.format(date_))
#             date_now = date_
#
#         if multiprocessing_switch:
#             pool.apply_async(func=one_day_one_stk, args=(stk, date_, this_log, path_to_check,))
#         else:
#             one_day_one_stk(stk, date_, this_log, path_to_check)
#
#     pool.close()
#     pool.join()
#             # venue = util.util.get_venue(stk)
#             # date = date_.replace('-', '')
#             # is_valid = util.common_data.check_data(date, stk, venue)
#             # if is_valid:
#             #     pass
#             #     # this_log.info('valid: {} {}'.format(date, stk))
#             # else:
#             #     this_log.info('start download {} {} {}'.format(date, stk, venue))
#             #     download_success = util.common_data.download_one_day_one_stk(path_out=path_to_check, venue=venue, date=date, stk=stk)
#             #     if download_success:
#             #         is_valid = util.common_data.check_data(date, stk, venue)
#             #         if is_valid:
#             #             this_log.info('end download {} {} {}'.format(path_to_check, date, stk, venue))
#             #             pass
#             #         else:
#             #             this_log.error('error in download {} {} {}'.format (date, stk, venue))
#             #     else:
#             #         this_log.error('download failed: {} {}'.format(date, stk))
#
#     trading_days = sorted(list(set(daily_ret_me_data['date'].to_list())))
#     for date_ in trading_days:
#         if date_ != date_now:
#             this_log.info('{} begin'.format(date_))
#             date_now = date_
#
#         date = date_.replace ('-', '')
#         venue = util.util.get_venue('000300')
#         is_valid = util.common_data.check_data(date, '000300', venue)
#         if is_valid:
#             pass
#             # this_log.info('valid: {} {}'.format(date, '000300'))
#         else:
#             this_log.info('start download {} {} {}'.format(date, '000300', venue))
#             download_success = util.common_data.download_one_day_one_stk(path_out=path_to_check, venue=venue, date=date, stk='000300')
#             if download_success:
#                 is_valid = util.common_data.check_data(date, '000300', venue)
#                 if is_valid:
#                     this_log.info('end download {} {} {}'.format (date, '000300', venue))
#                 else:
#                     this_log.error('error in download {} {} {}'.format (date, '000300', venue))
#             else:
#                 this_log.error('error: download failed {} {}'.format(date, '000300'))


def one_day_one_stk(stk_, date__, path_to_check_):
    this_log_ = log.log.LoggerFramework()
    # print('begin {} {}'.format(stk, date_))
    venue = util.util.get_venue (stk_)
    date = date__.replace('-', '')
    is_valid, reason = util.common_data.check_data (date, stk_, venue)
    if is_valid:
        pass
        # this_log_.info('valid: {} {}'.format(date, stk_))
    else:
        this_log_.info('start download {} {} {} {}'.format (date, stk_, venue, reason))
        download_success = util.common_data.download_one_day_one_stk (path_out=path_to_check_, venue=venue, date=date, stk=stk_)
        if download_success:
            is_valid, reason = util.common_data.check_data (date, stk_, venue)
            if is_valid:
                this_log_.info ('end download {} {} {}'.format (path_to_check_, date, stk_, venue))
                pass
            else:
                this_log_.error ('error in download {} {} {} {}'.format (date, stk_, venue, reason))
        else:
            this_log_.error ('download failed: {} {}'.format (date, stk_))


if __name__ == '__main__':
    # main()

    this_log = log.log.LoggerFramework ()

    pool = multiprocessing.Pool(processes=7)
    # multiprocessing_switch = False
    multiprocessing_switch = True

    path_to_check = my_path.path.one_min_bid_ask_path

    date_beg = '20140101'
    date_end = '20161231'
    this_log.info('date_begin: {}, date_end: {}'.format(date_beg, date_end))

    # daily_ret_me_data = sql.sql.get_daily_ret_me(date_beg, date_end)
    daily_ret_me_data = util.common_data.get_daily_ret_me_from_csv(date_beg, date_end)

    daily_ret_me_data = daily_ret_me_data.sort_values(by=['date', 'id'])
    # daily_ret_me_data.groupby('date').count()

    date_stk_pairs = zip(daily_ret_me_data['date'].tolist(), daily_ret_me_data['id'].tolist())

    date_list = sorted(set(daily_ret_me_data['date'].tolist()))
    date_now = None

    for date_, stk in date_stk_pairs:
    # for date_ in date_list:
    #     stk = '000300'
        if date_ != date_now:
            this_log.info('{} begin'.format(date_))
            date_now = date_

        if multiprocessing_switch:
            pool.apply_async(one_day_one_stk, (stk, date_, path_to_check,))
        else:
            one_day_one_stk(stk, date_, path_to_check)

    pool.close()
    pool.join()
    pool.terminate()