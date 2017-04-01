# -*- coding: utf-8 -*-
"""
Created on 2017/3/20 12:36

@version: python3.5
@author: qd
"""

import pandas as pd


p_in = 'C:\\Users\\qiding\\Desktop\\total_2016.txt'
p_out = 'C:\\Users\\qiding\\Desktop\\total_2016.csv'

# s = ''
with open(p_in, 'r') as f_in:
    s = f_in.read()
    # for l in f_in.readlines():
    #     s += f_in.read()

chunk_list = s.split('cite-key ')

data_df = pd.DataFrame()

for n, chunk in enumerate(chunk_list):
    print(n)
    article_pos = chunk.find('(article) ')

    if article_pos < 0:
        continue

    title = chunk[:article_pos]
    tail = chunk[article_pos+10:]
    sentences = tail.split('\n')
    keys = []
    contents = []

    keys.append('Title')
    contents.append(title)

    for i, sentence in enumerate(sentences):
        if len(sentence.strip()) == 0:
            continue

        if i % 2 == 0:
            keys.append(sentence)
        else:
            contents.append(sentence)
    if len(keys) != len(contents):
        print('error: {}, keys: {}, sentences: {}'.format(title, keys, sentences))
    data_s = pd.DataFrame(data=contents, index=keys).T
    data_df = data_df.append(data_s)

data_df.to_csv(p_out, index=None)
