# -*- coding: utf-8 -*-
"""
Created on Tue Oct 11 18:50:58 2016

@author: Лихотин
"""

import pandas as panda
from datetime import datetime

data1 = panda.read_csv('events_ios.csv')
data2 = panda.read_csv('events_android.csv')

file1 = panda.DataFrame
file1 = data1.drop(['android_id'], axis = 1)

file2 = panda.DataFrame
file2 = data2.drop(['ios_ifv'], axis = 1)

out = panda.merge(file1, file2, how='outer',on='event_datetime')

out.sort(['event_datetime'], inplace=True)
out.to_csv("merge.csv", header=True)
        
print (out.columns)