# -*- coding: utf-8 -*-
"""
Created on 2017/3/11 23:16

@version: python3.5
@author: qd
"""

import my_path.path
import util.common_data
import util.util
import util.const
import data_process.data_process_funcs as func
import sql.sql
import log.log

import multiprocessing
import os
import pandas as pd
import numpy as np
import random


def main():
    this_log = log.log.LoggerFramework ()

    begin_date = '20150101'
    begin_date_one_year_before = '20140101'
    end_date = '20161231'

    resample_freq = 1
    look_back_window = 252

    index = '000300'

    # jump_type = 'continuous'
    # jump_type = 'discontinuous'
    jump_type = 'overnight'

    # multiprocessing_switch = True
    multiprocessing_switch = False
    multiprocessing_num = 4

    if jump_type == 'continuous':
        output_path_root = my_path.path.continuous_beta_root
    elif jump_type == 'discontinuous':
        output_path_root = my_path.path.discontinuous_beta_root
    else:
        output_path_root = my_path.path.overnight_beta_root
    if not os.path.exists(output_path_root):
        os.makedirs(output_path_root)

    this_log.info('start reading daily ret me')
    daily_ret_me = util.common_data.get_daily_ret_me_from_csv(beg_date=begin_date_one_year_before, end_date=end_date)
    stk_list = sorted(set(daily_ret_me['id'].tolist()))
    this_log.info('end reading daily ret me')
    all_trading_days = sorted(set([date_.replace('-', '') for date_ in daily_ret_me['date'].tolist()]))

    this_log.info('reading market data')
    index_intraday_minute_data = func.get_intraday_minute_data(stk_code=index, begin_date=begin_date_one_year_before, end_date=end_date, date_list=all_trading_days)
    index_intraday_resample_data = func.resample_data(index_intraday_minute_data, resample_freq)
    index_intraday_resample_data = index_intraday_resample_data.rename(columns={'stk_ret': 'mkt_ret'})

    if jump_type == 'continuous':
        index_intraday_resample_data = func.get_rv_bv(index_intraday_resample_data, in_ret_col='mkt_ret', out_col_v_min='v_min_mkt', out_col_rv_0='rv_0_mkt')
    if jump_type == 'overnight':
        index_intraday_resample_data = func.generate_overnight(index_intraday_resample_data, 'mkt_ret')
    this_log.info('market data done: {}'.format(len(index_intraday_resample_data)))

    weight_data = sql.sql.get_weight_data (index_code='000300', begin_date=begin_date, end_date=end_date)
    stk_list = [str(stk_).zfill(6) for stk_ in sorted(set(weight_data['stkcd'].tolist()))]
    random.shuffle(stk_list)

    if multiprocessing_switch:
        pool = multiprocessing.Pool(processes=multiprocessing_num)
        for stk in stk_list:
            this_log.info('stk begin: {}'.format(stk))
            pool.apply_async(one_stk_func, (stk, begin_date_one_year_before, end_date, all_trading_days, resample_freq, index_intraday_resample_data, look_back_window, begin_date, output_path_root, jump_type,))
    else:
        for stk in stk_list:
            this_log.info('stk begin: {}'.format(stk))
            one_stk_func(stk, begin_date_one_year_before, end_date, all_trading_days, resample_freq, index_intraday_resample_data, look_back_window, begin_date, output_path_root, jump_type)


def one_stk_func(stk, begin_date_one_year_before, end_date, all_trading_days, resample_freq, index_intraday_resample_data, look_back_window, begin_date, output_path_root, jump_type):
    this_log = log.log.LoggerFramework ()
    data_out = pd.DataFrame(columns=['id', 'date', 'beta'])
    output_file_path = output_path_root + stk + '\\' + ('beta_continuous' if jump_type == 'continuous' else ('beta' if jump_type == 'overnight' else 'beta_jump')) + '.csv'
    if not os.path.exists(output_file_path):
        if not os.path.exists (output_path_root + stk + '\\'):
            this_log.info('make dirs: {}'.format(output_path_root + stk + '\\'))
            os.makedirs(output_path_root + stk + '\\')
        else:
            pass
    else:
        this_log.info('{} already done'.format(stk))
        return None
    stk_intraday_minute_data = func.get_intraday_minute_data (stk_code=stk, begin_date=begin_date_one_year_before,
                                                              end_date=end_date, date_list=all_trading_days)
    stk_intraday_resample_data = func.resample_data (stk_intraday_minute_data, resample_freq)

    if jump_type == 'continuous':
        stk_intraday_resample_data = func.get_rv_bv (stk_intraday_resample_data, in_ret_col='stk_ret', out_col_v_min='v_min_stk',
                                                     out_col_rv_0='rv_0_stk')
    if jump_type == 'overnight':
        stk_intraday_resample_data = func.generate_overnight(stk_intraday_resample_data, 'stk_ret')

    data_merged = func.merge_stk_data_and_index_data(index_intraday_resample_data, stk_intraday_resample_data)

    data_merged_dropna = data_merged.dropna()
    this_log.info ('stk data read done: {} {} {}'.format (stk, len (data_merged), len (data_merged_dropna)))
    for date, data_subsample in func.rolling_data(data_merged_dropna, look_back_window, begin_date):
        this_log.info ('begin {} {}'.format (date, stk))
        if jump_type == 'continuous':
            beta_cont = calculate_beta_continuous (data_subsample)
        elif jump_type == 'discontinuous':
            beta_cont = calculate_beta_jump(data_subsample)
        else:
            beta_cont = calculate_beta_jump(data_subsample)
        data_out = data_out.append (pd.Series ({'id': stk, 'date': date, 'beta': beta_cont}), ignore_index=True)
    this_log.info('data to csv: {} {}'.format (stk, output_file_path))
    data_out.to_csv(output_file_path, index=None)


def calculate_beta_continuous(sum_table):
    assert all(map(lambda x: x in sum_table.columns, ['mkt_ret', 'stk_ret', 'date', 'time'])), 'calculate beta jump assertion error: colume not completed'

    numerator1 = sum_table['stk_ret'] + sum_table['mkt_ret']
    numerator2 = sum_table['stk_ret'] - sum_table['mkt_ret']

    k_market = func.get_k(sum_table[['date', 'time', 'mkt_ret', 'v_min_mkt', 'rv_0_mkt']], 'rv_0_mkt', 'v_min_mkt')
    k_stk = func.get_k(sum_table[['date', 'time', 'stk_ret', 'v_min_stk', 'rv_0_stk']], 'rv_0_stk', 'v_min_stk')

    indicator1 = numerator1.abs() <= (k_market + k_stk)
    indicator2 = numerator2.abs() <= (k_stk - k_market)

    denominator = sum_table['mkt_ret']
    indicator3 = denominator <= k_market

    beta = ((numerator1 * numerator1)[indicator1].sum() - (numerator2 * numerator2)[indicator2].sum()) / (4 * (denominator * denominator)[indicator3].sum())

    return beta


def calculate_beta_jump(sum_table):
    assert all(map(lambda x: x in sum_table.columns, ['mkt_ret', 'stk_ret', 'date'])), 'calculate beta jump assertion error: colume not completed'
    sum_table['num'] = sum_table['stk_ret'] * sum_table['mkt_ret']
    sum_table['num'] = sum_table['num'] * sum_table['num']
    num = sum_table['num'].sum()

    sum_table.loc[:, 'deno'] = sum_table['mkt_ret'] * sum_table['mkt_ret'] * sum_table['mkt_ret'] * sum_table['mkt_ret']
    deno = sum_table['deno'].sum()

    beta = np.sqrt(num / deno)
    return beta


def calculate_beta_overnight(data_overnight):
    assert all(map(lambda x: x in data_overnight.columns, ['mkt_ret', 'stk_ret', 'date', 'time'])), 'calculate beta jump assertion error: colume not completed'
    data_overnight['num'] = data_overnight['stk_ret'] * data_overnight['mkt_ret']
    data_overnight['num'] = data_overnight['num'] * data_overnight['num']
    num = data_overnight['num'].sum()

    data_overnight.loc[:, 'deno'] = data_overnight['mkt_ret'] * data_overnight['mkt_ret'] * data_overnight['mkt_ret'] * data_overnight['mkt_ret']
    deno = data_overnight['deno'].sum()

    beta = np.sqrt(num / deno)
    return beta


if __name__ == '__main__':
    main()
