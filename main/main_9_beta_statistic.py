# -*- coding: utf-8 -*-
"""
Created on Mon Mar 13 09:47:40 2017

@author: wuyi
"""

import pandas as pd
import matplotlib.pyplot as plt

import my_path.path


# path = '\\\\LOGAN\\transport\\betas\\'
# path_vrank = '\\\\LOGAN\\transport\\beta_vrank\\'
path = my_path.path.beta_jump_data_path_root
path_vrank = path
path_out = path + 'beta_stats\\'


def get_beta():
    beta_c = pd.read_csv(path + 'continuous_beta_agg.csv').rename(columns = {'beta': 'beta_c'}).drop_duplicates(['id', 'date'])
    beta_d = pd.read_csv(path + 'discontinuous_beta_agg.csv').rename(columns = {'beta': 'beta_d'}).drop_duplicates(['id', 'date'])
    beta_n = pd.read_csv(path + 'overnight_beta_agg.csv').rename(columns = {'beta': 'beta_n'}).drop_duplicates(['id', 'date'])
    beta_s = pd.read_csv(path + 'standard_beta_agg.csv').rename(columns = {'beta': 'beta_s'}).drop_duplicates(['id', 'date'])

    beta_all = pd.merge(beta_c, beta_d, how='inner', on=['id', 'date'])
    beta_all = beta_all.merge(beta_n, how='inner', on=['id', 'date'])
    beta_all = beta_all.merge(beta_s, how='inner', on=['id', 'date']) 
    return beta_c, beta_d, beta_n, beta_s, beta_all

def get_corr(beta_all):   
    beta_corr = beta_all[['beta_c', 'beta_d', 'beta_n', 'beta_s']].corr()
    return beta_corr


def get_beta_vrank_corr(beta_vrank):
    vrank = beta_vrank[['id', 'date','v_port_c', 'v_port_s', 'v_port_n', 'v_port_d', ]]
    corr_each_day = vrank.groupby(['date'])['v_port_c', 'v_port_s', 'v_port_n', 'v_port_d'].corr()
    corr_each_day_resetindex = corr_each_day.reset_index()
    corr_mean = corr_each_day_resetindex.groupby(['level_1']).mean()
    return corr_mean


def get_beta_value_corr(beta_all):
    vrank = beta_all[['id', 'date','beta_c', 'beta_s', 'beta_n', 'beta_d', ]]
    corr_each_day = vrank.groupby(['date'])['date','beta_c', 'beta_s', 'beta_n', 'beta_d', ].corr()
    corr_each_day_resetindex = corr_each_day.reset_index()
    corr_mean = corr_each_day_resetindex.groupby(['level_1']).mean()
    return corr_mean


 
def get_stat(beta_c, beta_d, beta_n, beta_s):
    beta_stat = pd.DataFrame()
    for beta in [beta_c, beta_d, beta_n, beta_s]:
        temp_stat = beta.iloc[:,2].describe()
        beta_stat = beta_stat.append(temp_stat)
    return beta_stat  

def plot_density(beta_all):
    beta_all = beta_all[['beta_c', 'beta_d', 'beta_n', 'beta_s']]
    beta_all.plot(kind='kde', figsize=(20,10), xlim=(-1, 5))
    plt.savefig(path + 'beta_stat.png', dpi=150)

def get_beta_vrank():
    beta_vrank_c = pd.read_csv(path_vrank + 'continuous_beta_vrank.csv').rename(columns = {'beta': 'beta_c', 'v_port': 'v_port_c', 'lme': 'lme_c'})
    beta_vrank_d = pd.read_csv(path_vrank + 'discontinuous_beta_vrank.csv').rename(columns = {'beta': 'beta_d', 'v_port': 'v_port_d', 'lme': 'lme_d'})
    beta_vrank_n = pd.read_csv(path_vrank + 'overnight_beta_vrank.csv').rename(columns = {'beta': 'beta_n', 'v_port': 'v_port_n', 'lme': 'lme_n'})
    beta_vrank_s = pd.read_csv(path_vrank + 'standard_beta_vrank.csv').rename(columns = {'beta': 'beta_s', 'v_port': 'v_port_s', 'lme': 'lme_s'})
    
    beta_vrank = pd.merge(beta_vrank_c, beta_vrank_d, how='inner', on=['id', 'date'])
    beta_vrank = beta_vrank.merge(beta_vrank_n, how='inner', on=['id', 'date'])
    beta_vrank = beta_vrank.merge(beta_vrank_s, how='inner', on=['id', 'date'])
    return beta_vrank

def get_table3(beta_vrank):
    sorted_by_beta_c = beta_vrank[['beta_c', 'beta_d', 'beta_n', 'beta_s', 'lme_c']].groupby(beta_vrank['v_port_c']).mean()
    sorted_by_beta_d = beta_vrank[['beta_c', 'beta_d', 'beta_n', 'beta_s', 'lme_d']].groupby(beta_vrank['v_port_d']).mean()
    sorted_by_beta_n = beta_vrank[['beta_c', 'beta_d', 'beta_n', 'beta_s', 'lme_n']].groupby(beta_vrank['v_port_n']).mean()
    sorted_by_beta_s = beta_vrank[['beta_c', 'beta_d', 'beta_n', 'beta_s', 'lme_s']].groupby(beta_vrank['v_port_s']).mean()
    return sorted_by_beta_c, sorted_by_beta_d, sorted_by_beta_n, sorted_by_beta_s


if __name__ == '__main__':
    beta_c, beta_d, beta_n, beta_s, beta_all = get_beta()
    # beta_all.to_csv(path_out + 'beta_all.csv')
    beta_corr = get_beta_value_corr(beta_all)
    # beta_stat = get_stat(beta_c, beta_d, beta_n, beta_all[['id', 'date', 'beta_s']])

    beta_corr.to_csv(path_out + 'beta_corr.csv')
    # beta_stat.to_csv(path_out + 'beta_stat.csv')

    # plot_density(beta_all)
    
    # beta_vrank = get_beta_vrank()
    # beta_v_rank_corr = get_beta_vrank_corr(beta_vrank)
    # beta_v_rank_corr.to_csv(path_out + 'vrank_corr.csv')

    # sorted_by_beta_c, sorted_by_beta_d, sorted_by_beta_n, sorted_by_beta_s = get_table3(beta_vrank)

    # sorted_by_beta_c.to_csv(path_out + 'sorted_by_beta_c.csv')
    # sorted_by_beta_d.to_csv(path_out + 'sorted_by_beta_d.csv')
    # sorted_by_beta_n.to_csv(path_out + 'sorted_by_beta_n.csv')
    # sorted_by_beta_s.to_csv(path_out + 'sorted_by_beta_s.csv')




