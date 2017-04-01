# -*- coding: utf-8 -*-
"""
Created on 2017/1/19 17:03

@version: python3.5
@author: qd
"""

import pandas as pd
import pymysql
import time

import log.log
import util.sql_config

my_log = log.log.LoggerSQL()

HOST = util.sql_config.HOST
DB_NAME_INFO = util.sql_config.DB_NAME_INFO
DB_USER = util.sql_config.DB_USER
DB_PASS = util.sql_config.DB_PASS
DB_PORT = util.sql_config.DB_PORT
DB_CHAR = util.sql_config.DB_CHAR


year_db_name_mapping_table = {
    '2012': util.sql_config.DB_NAME_1214,
    '2013': util.sql_config.DB_NAME_1214,
    '2014': util.sql_config.DB_NAME_1214,
    '2015': util.sql_config.DB_NAME_15,
    '2016': util.sql_config.DB_NAME_16,
    '2017': util.sql_config.DB_NAME_1718,
    '2018': util.sql_config.DB_NAME_1718,
}


def get_trading_days():
    conn = pymysql.connect(host=HOST, db=DB_NAME_INFO, user=DB_USER, passwd=DB_PASS, port=DB_PORT, charset=DB_CHAR)
    trading_days = pd.read_sql('select * from t_date_list', conn)
    days_list = list(trading_days['t_date'].values)
    days_list2 = sorted(list(map(lambda x: x.strftime('%Y%m%d'), days_list)))
    my_log.info('get trading days: {}'.format(len(days_list2)))
    conn.close()
    return days_list2


def get_stk_list_all():
    conn = pymysql.connect(host=HOST, db=DB_NAME_INFO, user=DB_USER, passwd=DB_PASS, port=DB_PORT, charset=DB_CHAR)
    coid_list_raw = pd.read_sql('select * from coid_list_20170118', conn)
    coid_list = list(coid_list_raw['coid'].values)
    coid_list2 = sorted([c for c in coid_list
                         # if len(c) == 6 and any([c.startswith(s) for s in ['000', '002', '300', '600', '601', '603', '511']])])
                         if len(c) == 6 and any([c.startswith(s) for s in ['600', '601', '603']])])
    my_log.info('get coid list: {}'.format(len(coid_list2)))
    conn.close()
    return coid_list2


def get_hf_data(date_str, stk, data_type, mkt_type):
    mkt_type = mkt_type.lower()
    assert data_type in ['order', 'trans', 'tick']
    assert mkt_type in ['sh', 'sz']
    table_name = data_type + '_' + stk + '_' + mkt_type

    db_name = year_db_name_mapping_table[date_str[0:4]]

    def _inner(read_times):
        try:
            conn = pymysql.connect(host=HOST, db=db_name, user=DB_USER, passwd=DB_PASS, port=DB_PORT, charset=DB_CHAR)
            data = df_read_sql(sql_code='select * from {} where date="{}"'.format(table_name, date_str), conn=conn)
            conn.close()
            sql_error = None
        except Exception as e:
            my_log.error('read sql error: {}, {}, {}, {}, {}, read times: {}'.format(date_str, stk, data_type, mkt_type, e, read_times))
            data = None
            sql_error = e
        return data, sql_error

    read_times = 0
    while True:
        read_times += 1
        data, sql_error = _inner(read_times)
        if sql_error is None:
            break
        elif read_times >= 20:
            break
        else:
            time.sleep(5)
    return data

# def cursor(sql_code):
#     with conn.cursor() as cursor:
#         cursor.execute(sql_code)
#         result = cursor.fetchall()
#     return result

# def df_to_sql(df, table_name, if_exists='replace'):
#     my_log.info('df_to_sql: {}'.format(table_name))
#     if isinstance(df, pd.DataFrame):
#         pass
#     else:
#         df = pd.DataFrame(df)
#     df.to_sql(table_name, conn, 'mysql', if_exists=if_exists)


def df_read_sql(sql_code=None, table_name=None, conn=None):

    if sql_code is not None:
        # my_log.info('df_read_sql: {}'.format(sql_code))
        df2 = pd.read_sql(sql_code, conn)
        my_log.info('df_read_sql: {}'.format(sql_code))
    elif table_name is not None:
        # my_log.info('df_read_sql: {}'.format(table_name))
        df2 = pd.read_sql('select * from {}'.format(table_name), conn)
        my_log.info('df_read_sql: {}'.format(sql_code))
    else:
        raise ValueError

    return df2


def get_daily_ret_me(beg_date, end_date):  # input date format: '%Y%m%d', output date format: '%Y%m%d'
    db_name = 'lf_data'
    table_name = 'dailyretme_2142_00_ch50'

    beg_date_reformat = '{}-{}-{}'.format(beg_date[0:4], beg_date[4:6], beg_date[6:])
    end_date_reformat = '{}-{}-{}'.format(end_date[0:4], end_date[4:6], end_date[6:])

    conn = pymysql.connect(host=HOST, db=db_name, user=DB_USER, passwd=DB_PASS, port=DB_PORT, charset=DB_CHAR)
    data = df_read_sql(sql_code='select * from {} where date>="{}" and date<="{}"'.format(table_name, beg_date_reformat, end_date_reformat), conn=conn)
    conn.close()

    return data


def get_weight_data(index_code, begin_date, end_date):
    db_name = 'lf_data'
    table_name_dict = {'000300': 'shsz300_idx_dwt', '000016': 'sh50_idx_dwt', '000905': 'csi500_idx_dwt'}
    table_name = table_name_dict[index_code]

    beg_date_reformat = '{}-{}-{}'.format(begin_date[0:4], begin_date[4:6], begin_date[6:])
    end_date_reformat = '{}-{}-{}'.format(end_date[0:4], end_date[4:6], end_date[6:])

    conn = pymysql.connect(host=HOST, db=db_name, user=DB_USER, passwd=DB_PASS, port=DB_PORT, charset=DB_CHAR)
    data = df_read_sql(sql_code='select * from {} where enddt>="{}" and enddt<="{}"'.format(table_name, beg_date_reformat, end_date_reformat), conn=conn)
    conn.close()

    return data


def get_intraday_performance():
    db_name = 'intraday_account'
    table_name = 'neat_records'
    conn = pymysql.connect(host=HOST, db=db_name, user=DB_USER, passwd=DB_PASS, port=DB_PORT, charset=DB_CHAR)
    data = df_read_sql(sql_code='select * from {}'.format(table_name), conn=conn)
    conn.close()

    return data



if __name__ == '__main__':
    # data = get_hf_data(date_str='20170216', stk='002768', data_type='tick', mkt_type='sz')
    import datetime
    print(datetime.datetime.now())
    data = get_daily_ret_me(beg_date='20150101', end_date='20150301')
    print(len(data))
    print(datetime.datetime.now())
