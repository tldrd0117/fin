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
        mean = df[target].mean()
        std = df[target].std()
        df[target] = (df[target] - mean)/std
        print(mean, std)
        X = np.hstack([X.reshape(-1), df[target].values.reshape(-1)])
    y = np.hstack([y.reshape(-1),df['yield'].values.reshape(-1)])
    # X=np.array([df[targetList].values/length]).reshape(-1,factorLen)
    # y=np.array([(df['yield']).values]).reshape(-1,1)
X=X.reshape(-1,factorLen)
y=y.reshape(-1,1)
print(X.shape)
print(y.shape)
nanValues = np.argwhere(np.isnan(y))
print(nanValues)
X = np.delete(X, nanValues, axis=0)
y = np.delete(y, nanValues, axis=0)
# y = y[~nanValues, :]
nanValues2 = np.isnan(X)
X[np.isnan(X)] = 0
X[X < 0] = 0
print(X.shape)
print(y.shape)
# targetdf = df['roic'].dropna()
# X=np.array(targetdf.index) # 공부하는 시간
# y=targetdf.values.flatten() # 각 공부하는 시간에 맵핑되는 성적
print(X)
print(y)

#정규화
# mean = X.mean(axis=0)
# std = X.std(axis=0)
# X = (X - mean) / std


model=Sequential()
# model.add(Dropout(0.8, input_shape=(factorLen,)))
model.add(Dense(1, input_shape=(factorLen,), activation='relu', use_bias=False))
sgd=optimizers.SGD(lr=0.00005)
# 학습률(learning rate, lr)은 0.01로 합니다.
model.compile(optimizer=sgd ,loss='mse',metrics=['mse'])
# 옵티마이저는 경사하강법의 일종인 확률적 경사 하강법 sgd를 사용합니다.
# 손실 함수(Loss function)은 평균제곱오차 mse를 사용합니다.
model.summary()
model.fit(X,y, batch_size=30, epochs=200, shuffle=True)
for layer in model.layers: 
    print(pd.Series([y for x in layer.get_weights() for y in x], index=factors))
import matplotlib.pyplot as plt
plt.plot(X, model.predict(X), 'b', X,y, 'k.')
plt.show()
