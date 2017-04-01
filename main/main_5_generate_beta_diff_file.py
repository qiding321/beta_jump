# -*- coding: utf-8 -*-
"""
Created on 2017/3/13 0:07

@version: python3.5
@author: qd
"""

import pandas as pd
import my_path.path


def main():

    name1 = 'overnight'
    name2 = 'discontinuous'

    file_1 = my_path.path.beta_jump_data_path_root + name1 + '_beta_agg.csv'
    file_2 = my_path.path.beta_jump_data_path_root + name2 + '_beta_agg.csv'

    path_out = my_path.path.beta_jump_data_path_root + 'beta_diff_' + name1 + '_' + name2 + '.csv'

    data1 = pd.read_csv(file_1).rename(columns={'beta': 'beta1'})
    data2 = pd.read_csv(file_2).rename(columns={'beta': 'beta2'})

    data_merged = pd.merge(data1, data2, on=['date', 'id'])
    data_merged['beta_diff'] = data_merged['beta1'] - data_merged['beta2']

    data_output = data_merged[['date', 'id', 'beta_diff']].rename(columns={'beta_diff': 'beta'})
    data_output.to_csv(path_out, index=None)


if __name__ == '__main__':
    main()
