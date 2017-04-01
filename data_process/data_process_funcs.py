# -*- coding: utf-8 -*-
"""
Created on 2017/3/11 19:50

@version: python3.5
@author: qd
"""

import pandas as pd
import numpy as np
import datetime

import util.util
import util.const
import util.common_data
import log.log

this_log = log.log.LoggerBetaJump()


def resample_data(minute_data, resample_freq):  # todo

    if resample_freq == 1:
        return minute_data

    minute_data['idx'] = list(range(len(minute_data)))
    minute_data['idx'] /= resample_freq
    minute_data['idx'] = minute_data['idx'].astype(int)
    data_new = minute_data.groupby('idx')['stk_ret'].sum()
    data_new2 = minute_data.groupby('idx')[['time', 'date']].last()
    data_new2['stk_ret'] = data_new
    return data_new2


def merge_stk_data_and_index_data(index_data, stk_data):
    if 'time' in index_data.columns:
        data = pd.merge(stk_data, index_data, how='inner', on=['time', 'date'])
    else:
        data = pd.merge (stk_data, index_data, how='inner', on=['date'])
    return data


def rolling_data(data, look_back_window, begin_date):
    try:
        date_list = sorted(set(data['date'].tolist()))
        begin_date_index = [idx_ for idx_, date_ in enumerate(date_list) if date_ >= reformat_date_str(begin_date)][0]
        first_date_index = begin_date_index - look_back_window
        if first_date_index < 0:
            this_log.warning('warning: first date index <= 0, {} days before {} not found'.format(look_back_window, begin_date))

        idx1 = begin_date_index

        while idx1 <= len(date_list) - 1:

            idx0 = max(idx1 - look_back_window, 0)

            date0 = date_list[idx0]
            date1 = date_list[idx1]
            idx = (data['date'] <= date1) & (data['date'] >= date0)
            data_tmp = data[idx]
            yield date1, data_tmp
            idx0 += 1
            idx1 += 1

        raise StopIteration
    except Exception as e:
        this_log.error('rolling data error: {} {} {}'.format(begin_date, look_back_window, e))
        raise StopIteration


def reformat_date_str(date_str):  # '20150101' ==> '2015-01-01'
    s = '-'.join([date_str[:4], date_str[4:6], date_str[6:]])
    return s


def get_intraday_minute_data(stk_code, begin_date, end_date, date_list):  # date should be formatted as '%Y%m%d'
    stk_ret_table = pd.DataFrame()
    date_list_new = sorted(list(filter(lambda x: begin_date <= x <= end_date, date_list)))
    venue = util.util.get_venue(stk_code)
    for temp_date in date_list_new:
        data_path = util.common_data.get_data_path(temp_date, stk_code, venue)

        try:
            if stk_code in util.const.INDEX_CODE_LIST:
                temp_table = pd.read_csv(data_path, usecols=['time', 'date', 'price'])
                temp_table['prc_mid'] = temp_table['price']
            else:
                temp_table = pd.read_csv(data_path)

                if 'date' not in temp_table.columns:
                    temp_table['date'] = reformat_date_str(temp_date)
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
            stk_ret_table = stk_ret_table.append(temp_table)
        except Exception as e:
            this_log.error('error in data reading: {} {} {}'.format (temp_date, stk_code, e))
    return stk_ret_table


def get_rv_bv(data_df, in_ret_col, out_col_v_min, out_col_rv_0):
    idx_name = in_ret_col

    data_df['rv_0'] = data_df[idx_name] * data_df[idx_name]
    data_df['bv_0'] = (data_df[idx_name] * data_df[idx_name].shift(1)).abs() * np.pi / 2

    rv_by_date = data_df.groupby('date', as_index=False)['rv_0'].sum().rename(columns={'rv_0': 'rv', 'bv_0': 'bv'})
    bv_by_date = data_df.groupby('date', as_index=False)['bv_0'].sum().rename(columns={'rv_0': 'rv', 'bv_0': 'bv'})

    data_df = pd.merge(data_df, rv_by_date, on='date')
    data_df = pd.merge(data_df, bv_by_date, on='date')

    data_df = data_df.rename(columns={'rv_0': out_col_rv_0})

    data_df[out_col_v_min] = data_df[['rv', 'bv']].min(axis=1)

    data_df = data_df[['time', 'date', in_ret_col, out_col_v_min, out_col_rv_0]]
    return data_df


def get_k(data_df, rv_0_col, v_min_col):

    tau = 3
    w = 0.49

    idx_name = 'mkt_ret' if 'mkt_ret' in data_df.columns else 'stk_ret'

    # data_df['rv_0'] = data_df[idx_name] * data_df[idx_name]
    # data_df['bv_0'] = (data_df[idx_name] * data_df[idx_name].shift(1)).abs() * np.pi / 2
    #
    # rv_by_date = data_df.groupby('date', as_index=False)['rv_0'].sum().rename(columns={'rv_0': 'rv', 'bv_0': 'bv'})
    # bv_by_date = data_df.groupby('date', as_index=False)['bv_0'].sum().rename(columns={'rv_0': 'rv', 'bv_0': 'bv'})
    #
    # data_df = pd.merge(data_df, rv_by_date, on='date')
    # data_df = pd.merge(data_df, bv_by_date, on='date')
    #
    # data_df['v_min'] = data_df[['rv', 'bv']].min(axis=1)

    n = len(data_df['date'].drop_duplicates())

    def _cal_tod(data_):
        r = data_[rv_0_col][data_[idx_name] <= (tau * np.power(n, -w) * np.sqrt(data_[v_min_col]))].sum()
        return r

    tod_by_time = data_df.groupby('time').apply(_cal_tod) / _cal_tod(data_df) * n
    tod_by_time_df = pd.DataFrame(tod_by_time).reset_index().rename(columns={0: 'tod'})
    data_df = pd.merge(data_df, tod_by_time_df, on='time')

    k = tau * np.power(n, -w) * np.sqrt(data_df[v_min_col] * data_df['tod'])
    return k


def generate_overnight(data, ret_col):
    data_open = data.groupby('date', as_index=False)['log_prc'].first()
    data_close = data.groupby('date', as_index=False)['log_prc'].last().shift(1)
    ret_overnight = data_open['log_prc'] - data_close['log_prc']
    data_new = pd.concat([data_open['date'], ret_overnight], axis=1).rename(columns={'log_prc': ret_col})

    idx = (data_new[ret_col] <= .11) & (data_new[ret_col] >= -.11)
    data_new2 = data_new[idx]

    return data_new2


def sas_data_to_df(sas_data_path, date_cols):
    data = pd.read_sas(sas_data_path)
    data[date_cols] = data[date_cols].applymap(lambda x: datetime.datetime(1960, 1, 1) + datetime.timedelta(x))
    return data
