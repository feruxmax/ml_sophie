import pandas as pd
from sklearn.cross_validation import KFold
from sklearn.cross_validation import cross_val_score
from sklearn.cross_validation import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import StandardScaler

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression


def prepareXy(data):
    y = data['process_orderSendSuccess_event'].replace(2, 1).replace(3, 1)
    X = data.drop(['process_orderSendSuccess_event','ios_ifv','event_datetime'],
                 axis=1)
        
    return (X, y)
         
def crossval(X, y, clf, scoring):
    kf = KFold(y.size, n_folds=5, shuffle=True, random_state=1)
    score = cross_val_score(clf, X, y, scoring=scoring, cv=kf)    
    return score

def scale(X):
    scaler = StandardScaler()
    return scaler.fit_transform(X)
    
###############################################################################
models = {"boosting": GradientBoostingClassifier(n_estimators=500, random_state=241),
          "logistic": LogisticRegression(penalty='l2', C=0.5)}

dataset = pd.read_csv("evidance_event_ios.csv")
X, y = prepareXy(dataset)
X = scale(X)           
print(crossval(X, y, models['boosting'], 'precision').mean())
print(crossval(X, y, models['boosting'], 'recall').mean())

#%% 
X_learn, X_test, y_learn, y_test = train_test_split(X, y, test_size=0.4)
models["boosting"].fit(X_learn, y_learn)
forest_pred = models["boosting"].predict(X_test)
print(accuracy_score(forest_pred, y_test))