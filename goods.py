#%%
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np 
from sklearn import manifold

data = pd.read_csv('data/ml_requests.csv')

data.head()
#%%
items = data['items'].str.split(',')
features_names = set().union(*items)

#%%
features = pd.DataFrame(index=items.index, columns=features_names)

for i in items.index:
    features.iloc[i] = dict.fromkeys(items.iloc[i], 1)

features.fillna(0, inplace=True)
features = features.reindex_axis(sorted(features.columns), axis=1)
features

#%%
X = features
n_components=2

t0 = time()
mds = manifold.MDS(n_components, max_iter=100, n_init=1)
Y = mds.fit_transform(X)
t1 = time()
print("MDS: %.2g sec" % (t1 - t0))
plt.scatter(Y[:, 0], Y[:, 1])
plt.title("MDS (%.2g sec)" % (t1 - t0))
plt.axis('tight')
plt.show()

#%%
t0 = time()
tsne = manifold.TSNE(n_components=n_components, init='pca', random_state=0)
Y = tsne.fit_transform(X)
t1 = time()
print("t-SNE: %.2g sec" % (t1 - t0))
plt.scatter(Y[:, 0], Y[:, 1])
plt.title("t-SNE (%.2g sec)" % (t1 - t0))
plt.axis('tight')

plt.show()