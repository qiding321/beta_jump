# -*- coding: utf-8 -*-
"""
Created on 2017/3/12 9:32

@version: python3.5
@author: qd
"""


import os
import my_path.path
import pandas as pd
import numpy as np

# beta_jump_result_path = my_path.path.discontinuous_beta_root
#
#
# for f_ in os.listdir(beta_jump_result_path):
#     p_ = beta_jump_result_path + f_ + '\\beta_jump.csv'
#     d = pd.read_csv(p_)
#     d['beta'] = np.sqrt(d['beta'])
#     d.to_csv(p_, index=None)
#

# path_in = my_path.path.discontinuous_beta_root
# path_out = my_path.path.beta_jump_data_path_root + 'discontinuous_beta_agg.csv'
path_in = my_path.path.continuous_beta_root
path_out = my_path.path.beta_jump_data_path_root + 'continuous_beta_agg.csv'
# path_in = my_path.path.overnight_beta_root
# path_out = my_path.path.beta_jump_data_path_root + 'overnight_beta_agg.csv'

data = pd.DataFrame()

for f_ in os.listdir(path_in):
    p_ = path_in + f_ + '\\beta_continuous.csv'
    d = pd.read_csv(p_).rename(columns={'beta_cont': 'beta'})
    data = data.append(d)
data.to_csv(path_out, index=None)
