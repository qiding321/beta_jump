# -*- coding: utf-8 -*-
"""
Created on 2017/3/12 15:31

@version: python3.5
@author: qd
"""

import sql.sql
import my_path.path

import datetime
import pandas as pd
import numpy as np


def main():
    begin_date = '20100101'
    end_date = '20161231'

    begin_date_datetime = datetime.datetime.strptime(begin_date, '%Y%m%d')
    end_date_datetime = datetime.datetime.strptime(end_date, '%Y%m%d')

    begin_date_2 = begin_date_datetime.strftime('%Y-%m-%d')
    end_date_2 = end_date_datetime.strftime('%Y-%m-%d')

    # factor_csv_path = my_path.path.beta_jump_data_path_root + 'overnight_beta_agg.csv'
    # output_path = my_path.path.beta_jump_data_path_root + 'overnight_beta_vrank.csv'
    # factor_csv_path = my_path.path.beta_jump_data_path_root + 'discontinuous_beta_agg.csv'
    # output_path = my_path.path.beta_jump_data_path_root + 'discontinuous_beta_vrank.csv'
    factor_csv_path = my_path.path.beta_jump_data_path_root + 'standard_beta_agg.csv'
    output_path = my_path.path.beta_jump_data_path_root + 'standard_beta_long_term_all_market_vrank.csv'

    # file_descrip = 'beta_diff_overnight_standard'
    # factor_csv_path = my_path.path.beta_jump_data_path_root + file_descrip + '.csv'
    # output_path = my_path.path.beta_jump_data_path_root + file_descrip + '_vrank.csv'

    # weight_data = sql.sql.get_weight_data (index_code='000300', begin_date=begin_date, end_date=end_date)
    # date_list = sorted(set(weight_data.enddt.tolist()))

    # weight_data2 = weight_data.set_index(['enddt', 'stkcd'])

    # date_stklist_mapping = dict([(datetime.datetime(date_.year, date_.month, date_.day), weight_data[weight_data.enddt == date_].stkcd.tolist()) for date_ in date_list])

    dailyretme = sql.sql.get_daily_ret_me(begin_date, end_date)
    dailyretme['lme'] = dailyretme['eqy_sh_out'] * dailyretme['px_last']
    dailyretme['date'] = pd.to_datetime(dailyretme.date)

    factor_value_df = pd.read_csv(factor_csv_path)[['date', 'id', 'beta']].dropna().rename(columns={'beta_cont': 'beta'})
    factor_value_df = factor_value_df[(factor_value_df.date<=end_date_2) & (factor_value_df.date>=begin_date_2)]
    factor_value_df['id'] = factor_value_df['id'].apply(lambda x: int(x))
    factor_value_df['date'] = factor_value_df['date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
    factor_value_df = factor_value_df.sort_values(['date', 'id'])

    factor_value_df = pd.merge(left=factor_value_df, right=dailyretme[['date', 'id', 'lme']], on=['date', 'id'], how='left')
    groups = factor_value_df.groupby('date')
    # date, chunk = groups.__iter__().__next__()

    data_vrank = pd.DataFrame()
    for date, chunk in groups:
        # stk_list_300 = date_stklist_mapping[date.to_datetime()]
        chunk = chunk.drop_duplicates('id')
        size_qcut_idx = pd.qcut(chunk.lme, [0.0, .3, 1], labels=False)
        idx = size_qcut_idx == 1
        chunk_subset = chunk[idx]
        # chunk_subset = chunk[chunk.id.apply(lambda x: x in stk_list_70)].drop_duplicates('id')
        q_cut = pd.qcut(chunk_subset.beta, 10, labels=False)
        chunk_subset['v_port'] = q_cut
        data_vrank = data_vrank.append(chunk_subset)

    # data_vrank_size = pd.merge(left=data_vrank, right=dailyretme[['date', 'id', 'lme']], on=['date', 'id'], how='left')
    data_vrank_size = data_vrank
    data_vrank_size.to_csv(output_path, index=None)

if __name__ == '__main__':
    main()

