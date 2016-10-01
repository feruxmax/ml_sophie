# -*- coding: utf-8 -*-
"""
Created on Tue Sep 27 21:56:46 2016

@author: Лихотин
"""

import pandas as panda
from sklearn.neighbors import KNeighborsRegressor
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.svm import SVR
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score
from sklearn.cross_validation import train_test_split
import matplotlib.pyplot as plt

dataset = panda.read_csv("evidance_event_android.csv")

models = [LinearRegression(), # метод наименьших квадратов
	          RandomForestRegressor(n_estimators=100, max_features ='sqrt'), # случайный лес
	          KNeighborsRegressor(n_neighbors=6), # метод ближайших соседей
	          SVR(kernel='linear'), # метод опорных векторов с линейным ядром
	          LogisticRegression() # логистическая регрессия
	          ]
trg = dataset['process_orderSendSuccess_event'].replace(2, 1)
trn = dataset.drop(['process_orderSendSuccess_event','android_id','event_datetime'], axis=1)
Xtrn, Xtest, Ytrn, Ytest = train_test_split(trn, trg, test_size=0.4)
#создаем временные структуры
TestModels = panda.DataFrame()
tmp = {}
#для каждой модели из списка
for model in models:
    #получаем имя модели
    m = str(model)
    tmp['Model'] = m[:m.index('(')] 
    #l = len(Ytrn)
    #обучаем модель
    model.fit(Xtrn, Ytrn)
    #вычисляем коэффициент детерминации
    tmp['R2_Y1'] = r2_score(Ytest, model.predict(Xtest))
    #записываем данные и итоговый DataFrame
    TestModels = TestModels.append([tmp])
#делаем индекс по названию модели
TestModels.set_index('Model', inplace=True)
fig, axes = plt.subplots(ncols=1, figsize=(10,4))
#TestModels.R2_Y1.plot(ax=axes[0], kind='bar', title='R2_Y1')
TestModels.R2_Y1.plot(ax=axes, kind='bar', color='green', title='R2_Y2')

