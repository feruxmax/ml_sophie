# -*- coding: utf-8 -*-
"""
Created on Tue Oct 11 18:50:58 2016

@author: Лихотин
"""

import pandas as panda

data1 = panda.read_csv('events_ios.csv')
data2 = panda.read_csv('events_android.csv')


file1 = panda.DataFrame
file1 = data1.drop(['android_id'], axis = 1)

file2 = panda.DataFrame
file2 = data2.drop(['ios_ifv'], axis = 1)
'''
frames = [data1, data2]
out = panda.concat(frames)
#out = panda.merge(file1, file2, how='outer',on='event_datetime')

out.sort(['event_datetime'], inplace=True)

out.to_csv("merge.csv", header=True, encoding='utf-8')

out = panda.read_csv('merge.csv')
out['Identifier'] = ''
out = out.drop(['Unnamed: 0'], axis=1)

date = out.event_datetime
out['android_id'].replace('nan','')

for i, curr_day in enumerate(date):
    curr_line = {}
    curr_line = out.ix[i]
    if(out.ix[[i, 'android_id']] == 'nan'):
        out.ix[i,out['android_id']] = ''
    else:
        out.ix[i,out['ios_ifv']] = ''
        
out['Identifier'] = out['android_id'].astype(str) + out['ios_ifv'].astype(str)
'''
out = data1.append(data2, ignore_index=True)
out['ios_ifv'].fillna(out['android_id'], inplace=True)

out.drop(['android_id','google_aid', 'ios_ifa'], axis=1, inplace=True)

out.rename(index=str, columns={"ios_ifv": "Identifier"}, inplace=True)

out.sort(['event_datetime'], inplace=True)
out.to_csv("merge.csv", header=True, encoding='utf-8')
      
print (out.columns)