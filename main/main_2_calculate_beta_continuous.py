# -*- coding: utf-8 -*-
"""
Created on 2017/3/11 12:37

@version: python3.5
@author: qd
"""

import my_path.path
import util.common_data
import util.util
import util.const
import data_process.data_process_funcs as func

import os
import pandas as pd
import numpy as np

import log.log

this_log = log.log.LoggerBetaJump()


def main():
    output_path_root = my_path.path.continuous_beta_root
    if not os.path.exists(output_path_root):
        os.makedirs(output_path_root)

    begin_date = '20150101'
    begin_date_one_year_before = '20140101'
    end_date = '20161231'

    resample_freq = 1
    look_back_window = 252

    index = '000300'

    this_log.info('start reading daily ret me')
    daily_ret_me = util.common_data.get_daily_ret_me_from_csv(beg_date=begin_date_one_year_before, end_date=end_date)
    stk_list = sorted(set(daily_ret_me['id'].tolist()))
    this_log.info('end reading daily ret me')
    all_trading_days = sorted(set([date_.replace('-', '') for date_ in daily_ret_me['date'].tolist()]))

    this_log.info('reading market data')
    index_intraday_minute_data = func.get_intraday_minute_data(stk_code=index, begin_date=begin_date_one_year_before, end_date=end_date, date_list=all_trading_days)
    index_intraday_resample_data = func.resample_data(index_intraday_minute_data, resample_freq)
    index_intraday_resample_data = index_intraday_resample_data.rename(columns={'stk_ret': 'mkt_ret'})

    index_intraday_resample_data = func.get_rv_bv(index_intraday_resample_data, in_ret_col='mkt_ret', out_col_v_min='v_min_mkt', out_col_rv_0='rv_0_mkt')
    this_log.info('market data done: {}'.format(len(index_intraday_resample_data)))

    data_out = pd.DataFrame(columns=['id', 'date', 'beta_cont'])

    for stk in stk_list:
        this_log.info('stk begin: {}'.format(stk))
        stk_intraday_minute_data = func.get_intraday_minute_data(stk_code=stk, begin_date=begin_date_one_year_before, end_date=end_date, date_list=all_trading_days)
        stk_intraday_resample_data = func.resample_data(stk_intraday_minute_data, resample_freq)

        stk_intraday_resample_data = func.get_rv_bv(stk_intraday_resample_data, in_ret_col='stk_ret', out_col_v_min='v_min_stk', out_col_rv_0='rv_0_stk')

        data_merged = func.merge_stk_data_and_index_data(index_intraday_resample_data, stk_intraday_resample_data)

        data_merged_dropna = data_merged.dropna()
        this_log.info('stk data read done: {} {} {}'.format(stk, len(data_merged), len(data_merged_dropna)))
        for date, data_subsample in func.rolling_data(data_merged_dropna, look_back_window, begin_date):
            this_log.info('begin {} {}'.format(date, stk))
            beta_cont = calculate_beta_continuous(data_subsample)
            data_out = data_out.append(pd.Series({'id': stk, 'date': date, 'beta_cont': beta_cont}), ignore_index=True)
        this_log.info('data to csv: {} {}'.format(stk, output_path_root + stk + '\\' + 'beta_continuous.csv'))
        if not os.path.exists(output_path_root + stk + '\\'):
            os.makedirs(output_path_root + stk + '\\')
        data_out.to_csv(output_path_root + stk + '\\' + 'beta_continuous.csv', index=None)


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


if __name__ == '__main__':
    main()
