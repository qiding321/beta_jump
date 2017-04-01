# -*- coding: utf-8 -*-
"""
Created on 2017/3/13 8:50

@version: python3.5
@author: qd
"""

import util.common_data
import util.util
import util.const
import data_process.data_process_funcs as funcs
import my_path.path
import util.col_type_map

import pandas as pd
import numpy as np
import os
import random
import log.log


def main():

    this_log = log.log.LoggerFramework()

    begin_date = '20140101'
    end_date = '20161231'

    daily_ret_me = util.common_data.get_daily_ret_me_from_csv(beg_date=begin_date, end_date=end_date)
    all_trading_days = sorted (set ([date_.replace ('-', '') for date_ in daily_ret_me['date'].tolist ()]))

    data_out = pd.DataFrame()
    n = len(daily_ret_me)
    now_num = 0

    # row_list = list(daily_ret_me.iterrows())
    # random.shuffle(row_list)
    output_path = my_path.path.beta_jump_data_path_root + 'intrady_variation_faster.csv'
    this_log.info('output path: {}'.format(output_path))

    for k, v in daily_ret_me.iterrows():
        now_num += 1
        stk_code = v['id']
        temp_date = v['date'].replace('-', '')

        # if now_num % 1000 == 0:
        print(stk_code, temp_date, 'begin', '{}/{}'.format(now_num, n))

        venue = util.util.get_venue(stk_code)
        data_path = util.common_data.get_data_path(temp_date, stk_code, venue)
        temp_table = pd.read_csv(data_path)

        if 'date' not in temp_table.columns:
            temp_table['date'] = funcs.reformat_date_str(temp_date)
        temp_table = temp_table[['time', 'date', 'ask1', 'bid1']]

        temp_table['bid1'] = np.where(temp_table['bid1'] == 0, temp_table['ask1'], temp_table['bid1'])
        temp_table['ask1'] = np.where(temp_table['ask1'] == 0, temp_table['bid1'], temp_table['ask1'])

        temp_table['ask1'] /= util.const.TICK_SIZE
        temp_table['bid1'] /= util.const.TICK_SIZE

        temp_table['prc_mid'] = (temp_table['ask1'] + temp_table['bid1']) / 2
        temp_table = temp_table[temp_table['prc_mid'] > 0]

        idx_in_morning = (temp_table['time'] >= util.const.MARKET_OPEN_TIME_MORNING_STR) & (temp_table['time'] <= util.const.MARKET_END_TIME_MORNING_STR)
        idx_in_afternoon = (temp_table['time'] >= util.const.MARKET_OPEN_TIME_AFTERNOON_STR) & (temp_table['time'] <= util.const.MARKET_END_TIME_AFTERNOON_STR)
        temp_table = temp_table[idx_in_morning | idx_in_afternoon]

        temp_table['log_prc'] = np.log(temp_table['prc_mid'])
        temp_table['stk_ret'] = temp_table['log_prc'].diff()
        temp_table = temp_table[['time', 'date', 'stk_ret', 'log_prc']]

        variation = (temp_table.stk_ret * temp_table.stk_ret).sum()

        data_out = data_out.append(pd.Series({'id': stk_code, 'date': temp_date, 'variation': variation}), ignore_index=True)

        print(stk_code, temp_date, 'done')
        if now_num % 1000 == 0:
            data_out.to_csv(output_path)


def main_np():

    this_log = log.log.LoggerFramework()

    begin_date = '20140101'
    end_date = '20161231'

    daily_ret_me = util.common_data.get_daily_ret_me_from_csv(beg_date=begin_date, end_date=end_date)
    all_trading_days = sorted (set ([date_.replace ('-', '') for date_ in daily_ret_me['date'].tolist ()]))

    data_out = pd.DataFrame()
    n = len(daily_ret_me)
    now_num = 0

    # row_list = list(daily_ret_me.iterrows())
    # random.shuffle(row_list)
    output_path = my_path.path.beta_jump_data_path_root + 'intraday_variation_faster.csv'
    this_log.info('output path: {}'.format(output_path))

    t0 = util.const.MARKET_OPEN_TIME_MORNING_STR.encode()
    t1 = util.const.MARKET_END_TIME_MORNING_STR.encode()
    t2 = util.const.MARKET_OPEN_TIME_AFTERNOON_STR.encode()
    t3 = util.const.MARKET_END_TIME_AFTERNOON_STR.encode()

    daily_ret_me_row_list = [(k, v) for k, v in daily_ret_me.iterrows()]

    f_out = open(output_path, 'a')

    for k, v in daily_ret_me_row_list[1000999+1:]:
        now_num += 1
        stk_code = v['id']
        temp_date = v['date'].replace('-', '')

        if now_num % 1000 == 0:
            this_log.info('{} {} begin {}/{}'.format(stk_code, temp_date, now_num, n))

        venue = util.util.get_venue(stk_code)
        data_path = util.common_data.get_data_path(temp_date, stk_code, venue)
        try:
            temp_table = np.genfromtxt(data_path, names=[_[0] for _ in util.col_type_map.names_type_mapping], delimiter=',', dtype=util.col_type_map.names_type_mapping)
        except:
            try:
                temp_table = np.genfromtxt(data_path, names=[_[0] for _ in util.col_type_map.names_type_mapping2], delimiter=',', dtype=util.col_type_map.names_type_mapping2)
            except Exception as e:
                this_log.error('data error: {}, {}, {}, {}'.format(now_num, stk_code, temp_date, e))
                continue

        # temp_table = pd.read_csv(data_path)

        # if 'date' not in temp_table.columns:
        #     temp_table['date'] = funcs.reformat_date_str(temp_date)
        # temp_table = temp_table[['time', 'date', 'ask1', 'bid1']]

        temp_table['bid1'] = np.where(temp_table['bid1'] == 0, temp_table['ask1'], temp_table['bid1'])
        temp_table['ask1'] = np.where(temp_table['ask1'] == 0, temp_table['bid1'], temp_table['ask1'])

        temp_table['bid1'] = temp_table['bid1'].astype('float64')
        temp_table['ask1'] = temp_table['ask1'].astype('float64')

        temp_table['ask1'] = temp_table['ask1'] / util.const.TICK_SIZE
        temp_table['bid1'] = temp_table['bid1'] / util.const.TICK_SIZE

        prc_mid = (temp_table['ask1'] + temp_table['bid1']) / 2
        idx0 = prc_mid > 0

        idx_in_morning = (temp_table['time'] >= t0) & (temp_table['time'] <= t1)
        idx_in_afternoon = (temp_table['time'] >= t2) & (temp_table['time'] <= t3)
        prc_mid = prc_mid[(idx_in_morning | idx_in_afternoon) & idx0]

        log_px = np.log(prc_mid)
        stk_ret = np.diff(log_px)

        variation = (stk_ret * stk_ret).sum()

        # data_out = data_out.append(pd.Series({'id': stk_code, 'date': temp_date, 'variation': variation}), ignore_index=True)

        if now_num % 1000 == 0:
            this_log.info(stk_code + temp_date + 'done')

        # if now_num >= 15:
        #     break
        #
        # if now_num % 1000 == 0:
        #     data_out.to_csv(output_path)
        f_out.write(',{date},{stkcode},{intradayvariation}\n'.format(date=temp_date, stkcode=stk_code, intradayvariation=variation))

    f_out.close()


if __name__ == '__main__':
    # import cProfile
    # cProfile.run('main()')
    main_np()
