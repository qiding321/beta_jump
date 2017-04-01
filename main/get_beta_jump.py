# -*- coding: utf-8 -*-
"""
Created on Fri Mar 10 16:17:11 2017

@author: wuyi
"""

import os
import pandas as pd
import numpy as np

path = 'E:\\wuyi\\beta\\'

date_list = os.listdir(path + 'SH')

def get_stk_ret(stk, date, n):
    stk_ret_table = pd.DataFrame()
    for i in range(n)[::-1]:
        temp_date = date_list[date_list.index(date) - i]
        
        try:
            csv_name = path + 'SH\\' + temp_date + '\\' + stk + '.csv'
            temp_table = pd.read_csv(csv_name, usecols=['time', 'date', 'ask1', 'bid1'])
        except:
            csv_name = path + 'SZ\\' + temp_date + '\\' + stk + '.csv'
            temp_table = pd.read_csv(csv_name, usecols=['time', 'date', 'ask1', 'bid1'])
            
        temp_table['bid1'] = np.where(temp_table['bid1'] == 0, temp_table['ask1'], temp_table['bid1'])
        temp_table['ask1'] = np.where(temp_table['ask1'] == 0, temp_table['bid1'], temp_table['ask1'])
               
        temp_table['ask1'] = temp_table['ask1'] / 10000
        temp_table['bid1'] = temp_table['bid1'] / 10000     
                  
        temp_table['prc_mid'] = (temp_table['ask1'] + temp_table['bid1']) / 2
        temp_table = temp_table[temp_table['prc_mid'] > 0]
        temp_table['log_prc'] = np.log(temp_table['prc_mid'])
        temp_table['stk_ret'] = temp_table['log_prc'].diff()
        temp_table = temp_table[['time', 'date', 'stk_ret']]
        stk_ret_table = stk_ret_table.append(temp_table)
    return stk_ret_table

def get_mkt_ret(date, n):
    mkt_ret_table = pd.DataFrame()
    for i in range(n)[::-1]:
        temp_date = date_list[date_list.index(date) - i]
        csv_name = path + 'SH\\' + temp_date + '\\' + '000300' + '.csv'
        temp_table = pd.read_csv(csv_name, usecols=['time', 'date', 'price'])        
        temp_table['price'] = temp_table['price'] / 10000
        temp_table = temp_table[temp_table['price'] > 0]
        temp_table['log_prc'] = np.log(temp_table['price'])
        temp_table['mkt_ret'] = temp_table['log_prc'].diff()
        temp_table = temp_table[['time', 'date', 'mkt_ret']]
        mkt_ret_table = mkt_ret_table.append(temp_table)
    return mkt_ret_table   


def get_beta_jump(stk, date, n):
    stk_ret_table = get_stk_ret(stk, date, n)
    mtk_ret_table = get_mkt_ret(date, n)
    
    sum_table = pd.merge(stk_ret_table, mtk_ret_table, how='inner', on=['time', 'date'])    
    sum_table['num'] = sum_table['stk_ret'] * sum_table['mkt_ret']
    sum_table['num'] = sum_table['num'] * sum_table['num']
    num = sum_table['num'].sum()
    
    sum_table['deno'] = sum_table['mkt_ret'] * sum_table['mkt_ret'] * sum_table['mkt_ret'] * sum_table['mkt_ret']
    deno = sum_table['deno'].sum()
    
    beta = num / deno
    return beta


if __name__ == '__main__':
    date = '20140107'
    stk = '000005'
    n = 3
    stk_ret_table = get_stk_ret(stk, date, n)
    mtk_ret_table = get_mkt_ret(date, n)    
    beta = get_beta_jump(stk, date, n)
###############################################################################



#sum_table = pd.merge(stk_ret_table, mtk_ret_table, how='inner', on=['time', 'date'])    
#sum_table['num'] = sum_table['stk_ret'] * sum_table['mkt_ret']
#sum_table['num'] = sum_table['num'] * sum_table['num']
#num = sum_table['num'].sum()
#
#sum_table['deno'] = sum_table['mkt_ret'] * sum_table['mkt_ret'] * sum_table['mkt_ret'] * sum_table['mkt_ret']
#deno = sum_table['deno'].sum()









