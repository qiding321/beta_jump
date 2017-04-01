# -*- coding: utf-8 -*-
"""
Created on 2017/3/13 10:53

@version: python3.5
@author: qd
"""

import sql.sql
import my_path.path

import pandas as pd


output_path = my_path.path.beta_jump_data_path_root + 'beta_with_intraday_performance.csv'
output_path2 = my_path.path.beta_jump_data_path_root + 'beta_with_intraday_performance_corr.csv'

intraday_performance0 = sql.sql.get_intraday_performance()

intraday_performance = intraday_performance0.groupby(['t_date', 'coid'], as_index=False)['ret_before_cost'].sum()
intraday_performance = intraday_performance.rename(columns={'t_date': 'date', 'coid': 'id'})
intraday_performance.date = intraday_performance.date.apply(lambda x: '-'.join([str(xx).zfill(2) for xx in [x.year, x.month, x.day]]))
intraday_performance.id = intraday_performance.id.apply(int)
intraday_performance = intraday_performance.set_index(['date', 'id'])

beta_standard_path = my_path.path.beta_jump_data_path_root + 'standard_beta_agg.csv'
beta_continuous_path = my_path.path.beta_jump_data_path_root + 'continuous_beta_agg.csv'
beta_discontinuous_path = my_path.path.beta_jump_data_path_root + 'discontinuous_beta_agg.csv'
beta_overnight_path = my_path.path.beta_jump_data_path_root + 'overnight_beta_agg.csv'

beta_standard = pd.read_csv(beta_standard_path).drop_duplicates().rename(columns={'beta': 'beta_standard'}).sort_values(['id', 'date']).set_index(['date', 'id'])
beta_continuous = pd.read_csv(beta_continuous_path).drop_duplicates().rename(columns={'beta': 'beta_continuous'}).sort_values(['id', 'date']).set_index(['date', 'id'])
beta_discontinuous = pd.read_csv(beta_discontinuous_path).drop_duplicates().rename(columns={'beta': 'beta_discontinuous'}).sort_values(['id', 'date']).set_index(['date', 'id'])
beta_overnight = pd.read_csv(beta_overnight_path).drop_duplicates().rename(columns={'beta': 'beta_overnight'}).sort_values(['id', 'date']).set_index(['date', 'id'])

beta_standard_diff = (beta_standard - beta_standard .shift(1)).rename(columns={'beta_standard': 'beta_standard_diff'})
beta_continuous_diff = (beta_continuous - beta_continuous .shift(1)).rename(columns={'beta_continuous': 'beta_continuous_diff'})
beta_discontinuous_diff = (beta_discontinuous - beta_discontinuous .shift(1)).rename(columns={'beta_discontinuous': 'beta_discontinuous_diff'})
beta_overnight_diff = (beta_overnight - beta_overnight .shift(1)).rename(columns={'beta_overnight': 'beta_overnight_diff'})

beta_merged = pd.concat([
    beta_standard, beta_continuous, beta_discontinuous, beta_overnight,
    beta_standard_diff, beta_continuous_diff, beta_discontinuous_diff, beta_overnight_diff, intraday_performance
], axis=1)
beta_merged = beta_merged.dropna()
beta_merged.to_csv(output_path)
beta_merged.corr().to_csv(output_path2)

