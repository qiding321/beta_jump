
import sql.sql
import my_path.path
import log.log
import util.util
import util.common_data

import pandas as pd
import os
import multiprocessing


def main():
    date_beg = '20130101'
    date_end = '20131231'
    this_log = log.log.LoggerFramework()
    pool = multiprocessing.Pool(processes=10)
    # multiprocessing_switch = False
    multiprocessing_switch = True
    path_to_download = my_path.path.one_min_bid_ask_path
    this_log.info('date_begin: {}, date_end: {}'.format(date_beg, date_end))

    # daily_ret_me_data = sql.sql.get_daily_ret_me(date_beg, date_end)
    daily_ret_me_data = util.common_data.get_daily_ret_me_from_csv(date_beg, date_end)

    daily_ret_me_data = daily_ret_me_data.sort_values(by=['id', 'date'])
    # daily_ret_me_data.groupby('date').count()

    date_stk_pairs0 = zip(daily_ret_me_data['date'].tolist(), daily_ret_me_data['id'].tolist())
    date_list = sorted(set(daily_ret_me_data['date'].tolist()))
    date_index_pairs = zip(date_list, ['000300'] * len(date_list))
    date_stk_pairs1 = list(date_stk_pairs0) + list(date_index_pairs)

    date_now = None

    for date_, stk in date_stk_pairs1:
        if stk != date_now:
            this_log.info('{} begin'.format(stk))
            date_now = stk

        if multiprocessing_switch:
            pool.apply_async(one_day_one_stk, (stk, date_, path_to_download,))
        else:
            one_day_one_stk(stk, date_, path_to_download)

    pool.close()
    pool.join()
    pool.terminate()


def one_day_one_stk(stk_, date_, path_to_download):
    this_log_ = log.log.LoggerFramework()
    venue = util.util.get_venue (stk_)
    date = date_.replace('-', '')
    this_log_.info('start download {} {} {}'.format(date, stk_, venue))
    data_download_record(date_, stk_, 'begin')
    download_success = util.common_data.download_one_day_one_stk (path_out=path_to_download, venue=venue, date=date, stk=stk_)
    if download_success:
        this_log_.info('end download {} {} {}'.format(date, stk_, venue))
        data_download_record(date_, stk_, 'success')
    else:
        this_log_.info('failed download {} {} {}'.format(date, stk_, venue))
        data_download_record(date_, stk_, 'failed')


def data_download_record(date, stk, record_type):
    if record_type == 'begin':
        to_record_path = my_path.path.data_download_record_path + 'begin_record.csv'
    elif record_type == 'success':
        to_record_path = my_path.path.data_download_record_path + 'success_record.csv'
    elif record_type == 'failed':
        to_record_path = my_path.path.data_download_record_path + 'failed_record.csv'
    else:
        return

    with open(to_record_path, 'a') as f_out:
        f_out.write(date + ',' + stk + '\n')


if __name__ == '__main__':
    main()
