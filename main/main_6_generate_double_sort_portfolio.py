# -*- coding: utf-8 -*-
"""
Created on 2017/3/13 0:32

@version: python3.5
@author: qd
"""

import sql.sql
import my_path.path

import datetime
import pandas as pd


def main():
    begin_date = '20140101'
    end_date = '20161231'

    begin_date_datetime = datetime.datetime.strptime(begin_date, '%Y%m%d')
    end_date_datetime = datetime.datetime.strptime(end_date, '%Y%m%d')

    begin_date_2 = begin_date_datetime.strftime('%Y-%m-%d')
    end_date_2 = end_date_datetime.strftime('%Y-%m-%d')

    # factor_csv_path = my_path.path.beta_jump_data_path_root + 'overnight_beta_agg.csv'
    # output_path = my_path.path.beta_jump_data_path_root + 'overnight_beta_vrank.csv'
    # factor_csv_path = my_path.path.beta_jump_data_path_root + 'discontinuous_beta_agg.csv'
    # output_path = my_path.path.beta_jump_data_path_root + 'discontinuous_beta_vrank.csv'
    # factor_csv_path = my_path.path.beta_jump_data_path_root + 'standard_beta_agg.csv'
    # output_path = my_path.path.beta_jump_data_path_root + 'standard_beta_vrank.csv'

    # file_descrip1 = 'continuous'
    # file_descrip1 = 'discontinuous'
    # file_descrip1 = 'overnight'
    file_descrip1 = 'standard'
    # file_descrip2 = 'continuous'
    # file_descrip2 = 'discontinuous'
    # file_descrip2 = 'overnight'
    file_descrip2 = 'standard'

    print(file_descrip1, file_descrip2)

    factor_csv_path1 = my_path.path.beta_jump_data_path_root + file_descrip1 + '_beta_agg' + '.csv'
    factor_csv_path2 = my_path.path.beta_jump_data_path_root + file_descrip2 + '_beta_agg' + '.csv'
    # output_path = my_path.path.beta_jump_data_path_root + 'double_sort_' + file_descrip1 + '_' + file_descrip2 + '_vrank.csv'
    output_path = my_path.path.beta_jump_data_path_root + 'double_sort_' + 'size' + '_' + file_descrip2 + '_vrank.csv'

    weight_data = sql.sql.get_weight_data (index_code='000300', begin_date=begin_date, end_date=end_date)
    date_list = sorted(set(weight_data.enddt.tolist()))

    # weight_data2 = weight_data.set_index(['enddt', 'stkcd'])

    date_stklist_mapping = dict([(datetime.datetime(date_.year, date_.month, date_.day), weight_data[weight_data.enddt == date_].stkcd.tolist()) for date_ in date_list])

    dailyretme = sql.sql.get_daily_ret_me(begin_date, end_date)
    dailyretme['lme'] = dailyretme['eqy_sh_out'] * dailyretme['px_last']
    dailyretme['date'] = pd.to_datetime(dailyretme.date)

    factor_value_df1 = pd.read_csv(factor_csv_path1)[['date', 'id', 'beta']].dropna().rename(columns={'beta': 'beta1'})
    factor_value_df1['id'] = factor_value_df1['id'].apply (lambda x: int (x))
    factor_value_df1['date'] = factor_value_df1['date'].apply(lambda x: datetime.datetime.strptime (x, '%Y-%m-%d'))
    factor_value_df1 = pd.merge(left=factor_value_df1, right=dailyretme[['date', 'id', 'lme']], on=['date', 'id'], how='left')
    factor_value_df2 = pd.read_csv(factor_csv_path2)[['date', 'id', 'beta']].dropna().rename(columns={'beta': 'beta2'})
    factor_value_df2['id'] = factor_value_df2['id'].apply (lambda x: int (x))
    factor_value_df2['date'] = factor_value_df2['date'].apply (lambda x: datetime.datetime.strptime (x, '%Y-%m-%d'))
    factor_value_df = pd.merge(factor_value_df1, factor_value_df2, on=['date', 'id'])
    factor_value_df = factor_value_df[(factor_value_df.date<=end_date_2) & (factor_value_df.date>=begin_date_2)]

    factor_value_df = factor_value_df.sort_values(['date', 'id']).drop_duplicates(['date', 'id'])

    groups = factor_value_df.groupby('date')
    # date, chunk = groups.__iter__().__next__()

    data_vrank = pd.DataFrame()
    for date, chunk in groups:
        stk_list_300 = date_stklist_mapping[date.to_datetime()]
        chunk_subset = chunk[chunk.id.apply(lambda x: x in stk_list_300)].drop_duplicates('id')
        q_cut = pd.qcut(chunk_subset.lme, 5, labels=False)
        # q_cut = pd.qcut(chunk_subset.beta1, 5, labels=False)
        chunk_subset['v_port1'] = q_cut
        for v1, chunk_subset2 in chunk_subset.groupby('v_port1'):
            q_cut2 = pd.qcut(chunk_subset2.beta2, 5, labels=False)
            chunk_subset2['v_port2'] = q_cut2
            data_vrank = data_vrank.append(chunk_subset2)

    data_vrank['v_port'] = data_vrank['v_port1'] * 5 + data_vrank['v_port2']
    # data_vrank_size = pd.merge(left=data_vrank, right=dailyretme[['date', 'id', 'lme']], on=['date', 'id'], how='left')
    data_vrank_size = data_vrank
    data_vrank_size.to_csv(output_path, index=None)

if __name__ == '__main__':
    main()

