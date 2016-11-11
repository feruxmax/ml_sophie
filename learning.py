import pandas as pd
from sklearn.cross_validation import KFold
from sklearn.cross_validation import cross_val_score
from sklearn.cross_validation import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.metrics import precision_score
from sklearn.preprocessing import StandardScaler

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression

def prepareXy(data):
    y = data[TARGET].replace(2, 1).replace(3, 1)
    data.drop([TARGET, 'event_datetime'], axis=1, inplace=True)
    ids = data['id'].astype('category')
    data['id'] = ids.cat.codes

    return (data, y)
         
def crossval(X, y, clf, scoring):
    kf = KFold(y.size, n_folds=5, shuffle=True, random_state=1)
    score = cross_val_score(clf, X, y, scoring=scoring, cv=kf)    
    return score

def scale(X):
    scaler = StandardScaler()
    return scaler.fit_transform(X)
    
###############################################################################
TARGET = '_target'
T = 2 # hours
DELAY = 1 # Ts
PLATFORM = 'ios'
INFILE = "data/%s_T=%sh_d=%dT.csv" % (PLATFORM, T, DELAY)

models = {"boosting": GradientBoostingClassifier(n_estimators=500, random_state=241),
          "logistic": LogisticRegression(penalty='l2', C=0.5)}

print("learning ", INFILE)

dataset = pd.read_csv(INFILE)
X, y = prepareXy(dataset)
#X = scale(X)           
print("presision: ", crossval(X, y, models['boosting'], 'precision').mean())
print("recall: ", crossval(X, y, models['boosting'], 'recall').mean())
print("roc_auc: ", crossval(X, y, models['boosting'], 'roc_auc').mean())
#%% 
X_learn, X_test, y_learn, y_test = train_test_split(X, y, test_size=0.3)
models["boosting"].fit(X_learn, y_learn)
forest_pred = models["boosting"].predict(X_test)
print("accuracy: ", accuracy_score(forest_pred, y_test))
pred = pd.Series(forest_pred)
print('test: \n', y_test.value_counts())
print('pred: \n', pred.value_counts())
print('all:  \n', y.value_counts())
print("precision: ", precision_score(forest_pred, y_test))