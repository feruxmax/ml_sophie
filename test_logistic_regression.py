# -*- coding: utf-8 -*-
"""
Created on Wed Sep 21 19:11:53 2016
    
@author: Лихотин
`"""
import pandas as panda
from sklearn.linear_model import LogisticRegression
from sklearn.cross_validation import KFold
from sklearn.cross_validation import *

def score_logistic(X, y, C):
    clf = LogisticRegression(penalty='l2', C=C)
    kf = KFold(y.size, n_folds=5, shuffle=True, random_state=1)
    score = cross_val_score(clf, X, y, scoring='roc_auc', cv=kf)    
    return score.mean()


dataset = panda.read_csv("evidance_event_android.csv")

trg = dataset['process_orderSendSuccess_event'].replace(2, 1)
trn = dataset.drop(['process_orderSendSuccess_event','android_id','event_datetime'], axis=1)
#toto = trg.index()
#togo = trn.index()
rezult = score_logistic(trn, trg, 0.5)
print(rezult)
'''    
scaler = StandardScaler()
scaler.fit(X)
X_orig_scale = scaler.transform(X)
'''