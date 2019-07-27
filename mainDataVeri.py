# In[0]
from keras.models import Sequential # 케라스의 Sequential()을 임포트
from keras.layers import Dense, Dropout # 케라스의 Dense()를 임포트
from keras import optimizers # 케라스의 옵티마이저를 임포트
import numpy as np # Numpy를 임포트
import pandas as pd


factors = ['per', 'pcr', 'pbr', 'roe', 'psr', 'roic', 'eps', 'ebit', 'ev_ebit', 'ev_sales', 'ev_ebitda', '당기순이익률', '영업이익률', '매출총이익률', '배당수익률']
factorLen = len(factors)

X = np.array([])
y = np.array([])
for year in range(2007,2019):
    df = pd.read_hdf('h5data/factor_values' + str(year) + '.h5', key='df')
    length = len(df.index)
    # factors = ['per', 'pcr', 'pbr', 'roe', '당기순이익', '영업활동으로인한현금흐름', '투자활동으로인한현금흐름', '재무활동으로인한현금흐름', 'psr', 'roic', 'eps', 'ebit', 'ev_ebit', 'ev_sales', 'ev_ebitda', '당기순이익률', '영업이익률', '매출총이익률', '배당수익률', '매출액']

    targetList = list(map(lambda x : x+'_value', factors))
    allList = ['yield'] + targetList
    df = df[allList]
    for target in targetList:
        # mean = df[target].mean()
        # std = df[target].std()
        # df[target] = (df[target] - mean)/std
        X = np.hstack([X.reshape(-1), df[target].values.reshape(-1)])
    y = np.hstack([y.reshape(-1),df['yield'].values.reshape(-1)])
    # X=np.array([df[targetList].values/length]).reshape(-1,factorLen)
    # y=np.array([(df['yield']).values]).reshape(-1,1)
X=X.reshape(-1,factorLen)
X[np.isnan(X)] = 0
y=y.reshape(-1,1)
y[np.isnan(y)] = 0
# targetdf = df['roic'].dropna()
# X=np.array(targetdf.index) # 공부하는 시간
# y=targetdf.values.flatten() # 각 공부하는 시간에 맵핑되는 성적
print(X)
print(y)
# In[1]:
# X[0][1]
# print(X)
# print(X - X.mean(axis=0))
np.set_printoptions(suppress=True)
factors = ['per', 'pcr', 'pbr', 'roe', 'psr', 'roic', 'eps', 'ebit', 'ev_ebit', 'ev_sales', 'ev_ebitda', '당기순이익률', '영업이익률', '매출총이익률', '배당수익률']
arr = (((X - X.mean(axis=0))*(y-y.mean(axis=0))).sum(axis=0)/len(X))
print(arr)
for i in arr.argsort():
    print(factors[i])
# 8


#%%
