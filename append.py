# -*- coding: utf-8 -*-
"""
Created on Sun Nov  6 01:27:24 2016

@author: Лихотин
"""

import pandas as panda
import numpy as np

data1 = panda.read_csv('evidance_event.csv')
data2 = panda.read_csv('evidance_event2.csv')

out = data1.append(data2, ignore_index=True)
out.sort(['event_datetime'], inplace=True)

date = out['event_datetime']

for i, curr_date in enumerate(date):
    if(out.ix[i,'process_orderSendSuccess_event'] > 1):
        out.ix[i,'process_orderSendSuccess_event'] = 1

out.replace(np.nan, 0, inplace = True)

out.to_csv("evidance_event3.csv", header=True, encoding='utf-8')