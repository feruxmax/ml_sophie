#%%
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np 
from sklearn import manifold
from sklearn.cluster import KMeans
from sklearn.cluster import MeanShift 
from sklearn.cluster import AgglomerativeClustering
from time import time
#%%
data = pd.read_csv('data/ml_requests.csv')

data.head()
#%%
items = data['items'].str.split(',')
features_names = set().union(*items)

##%%
orders = pd.DataFrame(index=items.index, columns=features_names)

for i in items.index:
    orders.iloc[i] = dict.fromkeys(items.iloc[i], 1)

orders.fillna(0, inplace=True)
orders = orders.reindex_axis(sorted(orders.columns), axis=1)
orders.head()

#%%
user_ids = data['id_creator'].unique()
features = pd.DataFrame(index=user_ids, columns=orders.columns)

for i in features.index:
    features.loc[i] = orders[data['id_creator']==i].sum()

features.head()

#%%
X = features
n_components=2

#colors = KMeans(n_clusters=2).fit_predict(X)
#colors = MeanShift().fit_predict(X)
colors = AgglomerativeClustering(n_clusters=2).fit_predict(X)
colors

##%
t0 = time()
mds = manifold.MDS(n_components, max_iter=100, n_init=1)
Y = mds.fit_transform(X)
t1 = time()
print("MDS: %.2g sec" % (t1 - t0))
plt.scatter(Y[:, 0], Y[:, 1], c=colors, cmap=plt.cm.Spectral)
plt.title("MDS (%.2g sec)" % (t1 - t0))
plt.axis('tight')
plt.show()

#%%
t0 = time()
tsne = manifold.TSNE(n_components=n_components, init='pca', random_state=0)
Y = tsne.fit_transform(X)
t1 = time()
print("t-SNE: %.2g sec" % (t1 - t0))
plt.scatter(Y[:, 0], Y[:, 1], c=colors, cmap=plt.cm.Spectral)
plt.title("t-SNE (%.2g sec)" % (t1 - t0))
plt.axis('tight')
plt.show()