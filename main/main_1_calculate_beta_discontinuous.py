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

import os
import pandas as pd
import data_process.data_process_funcs as funcs
import log.log

this_log = log.log.LoggerBetaJump()


def main():
    output_path_root = my_path.path.discontinuous_beta_root
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

    index_intraday_minute_data = funcs.get_intraday_minute_data(stk_code=index, begin_date=begin_date_one_year_before, end_date=end_date, date_list=all_trading_days)
    index_intraday_resample_data = funcs.resample_data(index_intraday_minute_data, resample_freq)
    index_intraday_resample_data = index_intraday_resample_data.rename(columns={'stk_ret': 'mkt_ret'})

    data_out = pd.DataFrame(columns=['id', 'date', 'beta_jump'])

    for stk in stk_list:
        this_log.info('stk {} begin'.format(stk))

        stk_intraday_minute_data = funcs.get_intraday_minute_data(stk_code=stk, begin_date=begin_date_one_year_before, end_date=end_date, date_list=all_trading_days)
        stk_intraday_resample_data = funcs.resample_data(stk_intraday_minute_data, resample_freq)
        data_merged = funcs.merge_stk_data_and_index_data(index_intraday_resample_data, stk_intraday_resample_data)

        data_merged_dropna = data_merged.dropna()

        for date, data_subsample in funcs.rolling_data(data_merged_dropna, look_back_window, begin_date):
            beta_jump = calculate_beta_jump(data_subsample)
            data_out = data_out.append(pd.Series({'id': stk, 'date': date, 'beta_jump': beta_jump}), ignore_index=True)
        this_log.info('data to csv: {} {}'.format(stk, output_path_root + stk + '\\' + 'beta_continuous.csv'))
        data_out.to_csv(output_path_root + stk + '\\' + 'beta_jump.csv')


def calculate_beta_jump(sum_table):
    assert all(map(lambda x: x in sum_table.columns, ['mkt_ret', 'stk_ret', 'date', 'time'])), 'calculate beta jump assertion error: colume not completed'
    sum_table['num'] = sum_table['stk_ret'] * sum_table['mkt_ret']
    sum_table['num'] = sum_table['num'] * sum_table['num']
    num = sum_table['num'].sum()

    sum_table.loc[:, 'deno'] = sum_table['mkt_ret'] * sum_table['mkt_ret'] * sum_table['mkt_ret'] * sum_table['mkt_ret']
    deno = sum_table['deno'].sum()

    beta = num / deno
    return beta


if __name__ == '__main__':
    main()
