# In[0]:
from simulator.StockLoader import StockLoader
sl = StockLoader.create()
stockdf = sl.loadStockDf()
factordf = sl.loadFactor()

# In[1]: 액면

def isNumber(val):
    return isinstance(val, (int, float, complex)) 

import pandas as pd

priceLimitDate = pd.to_datetime('2015-06-15', format='%Y-%m-%d')
length = len(stockdf.index)
for index in stockdf.index:
    idx = stockdf.index.get_loc(index)
    if not isNumber(idx) or idx >= length - 1 :
        break
    if priceLimitDate > index:
        targets = stockdf.iloc[idx][stockdf.iloc[idx] * 1.25 <= stockdf.iloc[idx+1]].index
        if len(targets) >= 0:
            ratio = (stockdf.iloc[idx+1] / stockdf.iloc[idx])[targets]
            stockdf.iloc[:idx][targets] = stockdf.iloc[:idx][targets] * ratio
    elif priceLimitDate <= index:
        targets = stockdf.iloc[idx][stockdf.iloc[idx] * 1.3 <= stockdf.iloc[idx+1]].index
        if len(targets) >= 0:
            ratio = (stockdf.iloc[idx+1] / stockdf.iloc[idx])[targets]
            stockdf.iloc[:idx][targets] = stockdf.iloc[:idx][targets] * ratio
   

# if (current < priceLimitDate and stock['money'] * 1.25 <= latelyPrice or stock['money'] * 0.75 >= latelyPrice)\
#             or (current >= priceLimitDate and stock['money'] * 1.3 < latelyPrice or stock['money'] * 0.7 > latelyPrice):
#             ratio = latelyPrice / stock['money']
#             idx = np.argmin(np.abs(parValues - ratio))
#             stock['quantity'] = int(stock['quantity']/parValues[idx])

# In[1]:
import pandas as pd


# mdf = stockdf.resample('M').mean()
# shiftdf = mdf.shift(1)
# muddf = (mdf - shiftdf) / shiftdf
# muddf
# valuedf = pd.DataFrame(columns=list(range(1,31)))
# namedf = pd.DataFrame(columns=list(range(1,31)))
# for idx in muddf.index:
#     sortdf = muddf.loc[idx].sort_values(ascending=False)
#     namedf.loc[idx] = sortdf.index
#     valuedf.loc[idx] = sortdf.values
# sumdf = (stockdf['2015-12-30'] - stockdf['2015-01-02'])/stockdf['2015-01-02']
sumdf = (stockdf.resample('A').last() - stockdf.resample('A').first())/ stockdf.resample('A').first()

# sumdf = pd.Series((stockdf['2018-12-30'].values[0] - stockdf['2008-01-02'].values[0])/stockdf['2008-01-02'].values[0], index=stockdf['2018-12-30'].columns.values)
# sumdf
# stockdf
# In[2]:
import numpy as np
import pandas as pd
# sumdf = muddf['2017-01-01':'2017-12-31'].sum().sort_values(ascending=False)
# sumdf = sumdf.dropna()
for key in factordf.keys():
    factordf[key] = factordf[key].set_index(['종목명'])
factordf
# In[3]:
for key in factordf.keys():
    factordf[key].columns = list(map(lambda col : float(col) if isNumber(col) or col.isnumeric() else col, factordf[key].columns))
# In[3]: 증가율
# print(factordf.keys())
# keys = list(factordf.keys())
# for factorKey in keys:
#     onedf = factordf[factorKey]
#     compdf = onedf.shift(-1, axis=1)
#     compdf['종목명'] = np.nan
#     compdf['결산월'] = np.nan
#     compdf['단위'] = np.nan
#     targetdf = onedf - compdf
#     # print(onedf)
#     # targetdf['종목명'] = onedf['종목명']
#     factordf[factorKey+'증가율'] = targetdf
# print(factordf.keys())

# # In[4]: 확인
# print(factordf.keys())
# factordf['영업활동으로인한현금흐름증가율']

# In[4]: 종목별로 팩터를 구하고 팩터가 없는 종목은 삭제
def getFactor(factor, year):
    return factordf[factor][year]
def toNumeric(df):
    return df.applymap(lambda v : float(v) if isNumber(v) or v.isnumeric() else np.nan )
def toNumericSeries(series):
    return series.apply(lambda v : float(v) if isNumber(v) or v.isnumeric() else np.nan )
def getFactorMean(factor, ascending, year):
    factorValdf = toNumericSeries(getFactor(factor, year))
    intersect = list(set(factorValdf.index) & set(sumdf[str(year)].columns))
    factorValdf = factorValdf.loc[intersect].sort_values(ascending=ascending)
    meanList = []
    codeList = []
    for idx in range(0, factorValdf.size):
        code = factorValdf.index[idx]
        # print(code)
        # print(code, idx, factorValdf.iloc[idx])
        meanList.append([idx, factorValdf.iloc[idx]])
        codeList.append(code)
    plotdf = pd.DataFrame(meanList, columns=[factor+'_rank', factor+'_value'], index=codeList)
    return toNumeric(plotdf)
def addFactor(name, df, year):
    return pd.concat([df, getFactorMean(name, True, year) ], axis=1)
    

# factors = ['per', 'pcr', 'pbr', 'roe', '당기순이익', '영업활동으로인한현금흐름', '투자활동으로인한현금흐름', '재무활동으로인한현금흐름', 'psr', 'roic', 'eps', 'ebit', 'ev_ebit', 'ev_sales', 'ev_ebitda', '당기순이익률', '영업이익률', '매출총이익률', '배당수익률', '매출액']
factors = ['per', 'pcr', 'pbr', 'roe', 'psr', 'roic', 'eps', 'ebit', 'ev_ebit', 'ev_sales', 'ev_ebitda', '당기순이익률', '영업이익률', '매출총이익률', '배당수익률']
for year in range(2007,2019):   
    datadf = pd.DataFrame(np.reshape(sumdf[str(year)].values,(len(sumdf[str(year)].columns),1)), index=sumdf[str(year)].columns, columns=['yield'])
    for factor in factors:
        datadf = addFactor(factor, datadf, year)
    print(datadf)
    datadf.to_hdf('h5data/factor_values'+str(year)+'.h5', key='df', mode='w')
# df2 = getFactorMean('pcr', False)
# df3 = getFactorMean('pbr', False)
# df3 = getFactorMean('psr', False)
# df3 = getFactorMean('eps', False)
# df3 = getFactorMean('evit', False)
# df3 = getFactorMean('ev_ebit', False)
# df3 = getFactorMean('ev_ebitda', False)
# df3 = getFactorMean('ev_sales', False)
# df3 = getFactorMean('매출액', False)
# df3 = getFactorMean('당기순이익', False)
# df3 = getFactorMean('영업이익률', False)
# df3 = getFactorMean('배당수익률', False)
# df3 = getFactorMean('매출총이익률', False)
# df3 = getFactorMean('당기순수익률', False)
# df3 = getFactorMean('영업활동으로인한현금흐름', False)
# df3 = getFactorMean('재무활동으로인한현금흐름', False)
# df3 = getFactorMean('투자활동으로인한현금흐름', False)


# df4 = getFactorMean('pbr', False)
# plotdf = pd.concat([df1,df2,df3,df4], axis=1)
# # plotdf['per+pcr'] = (plotdf['per'] + plotdf['pcr'])/2
# # plotdf['per+pcr+pbr'] = (plotdf['per'] + plotdf['pcr']+ plotdf['pbr'])/2
# plotdf = plotdf.dropna()
# print(plotdf)
# plotdf.plot(figsize = (18,12), fontsize=12)
# plotdf.to_hdf('h5data/roic_pcr_roe_pbr_values', key='df', mode='w')
# plotdf
# In[3]: 기울기 구하기
# from keras.models import Sequential # 케라스의 Sequential()을 임포트
# from keras.layers import Dense # 케라스의 Dense()를 임포트
# from keras import optimizers # 케라스의 옵티마이저를 임포트
# import numpy as np # Numpy를 임포트
# newdf = df1.dropna()
# X=np.array(newdf.index) # 공부하는 시간
# y=newdf.values.flatten() # 각 공부하는 시간에 맵핑되는 성적
# # print(X)
# # print(y)
# model=Sequential()
# model.add(Dense(1, input_dim=1, activation='linear'))
# sgd=optimizers.SGD(lr=0.00001)
# # 학습률(learning rate, lr)은 0.01로 합니다.
# model.compile(optimizer=sgd ,loss='mse',metrics=['mse'])
# # 옵티마이저는 경사하강법의 일종인 확률적 경사 하강법 sgd를 사용합니다.
# # 손실 함수(Loss function)은 평균제곱오차 mse를 사용합니다.
# model.fit(X,y, batch_size=1, epochs=300, shuffle=False)

# import matplotlib.pyplot as plt
# plt.plot(X, model.predict(X), 'b', X,y, 'k.')

#%%
