from keras.models import Sequential # 케라스의 Sequential()을 임포트
from keras.layers import Dense, Dropout # 케라스의 Dense()를 임포트
from keras import optimizers # 케라스의 옵티마이저를 임포트
import numpy as np # Numpy를 임포트
import pandas as pd

df = pd.read_hdf('h5data/factor_values', key='df')
length = len(df.index)
factors = ['per', 'pcr', 'pbr', 'roe', '당기순이익', '영업활동으로인한현금흐름', '투자활동으로인한현금흐름', '재무활동으로인한현금흐름', 'psr', 'roic', 'eps', 'ebit', 'ev_ebit', 'ev_sales', 'ev_ebitda', '당기순이익률', '영업이익률', '매출총이익률', '배당수익률', '매출액']
targetList = list(map(lambda x : x+'_rank', factors))
allList = ['yield'] + targetList
factorLen = len(factors)

df = df[allList].dropna()
X=np.array([df[targetList].values/length]).reshape(-1,factorLen)
y=np.array([(df['yield']).values]).reshape(-1,1)
# targetdf = df['roic'].dropna()
# X=np.array(targetdf.index) # 공부하는 시간
# y=targetdf.values.flatten() # 각 공부하는 시간에 맵핑되는 성적
print(X)
print(y)
model=Sequential()
model.add(Dropout(0.8, input_shape=(factorLen,)))
model.add(Dense(1, activation='linear', use_bias=False))
sgd=optimizers.SGD(lr=0.0001)
# 학습률(learning rate, lr)은 0.01로 합니다.
model.compile(optimizer=sgd ,loss='mse',metrics=['mse'])
# 옵티마이저는 경사하강법의 일종인 확률적 경사 하강법 sgd를 사용합니다.
# 손실 함수(Loss function)은 평균제곱오차 mse를 사용합니다.
model.summary()
model.fit(X,y, batch_size=1, epochs=500, shuffle=True)

for layer in model.layers: print(layer.get_weights())

import matplotlib.pyplot as plt
plt.plot(X, model.predict(X), 'b', X,y, 'k.')
plt.show()
