# In[0]:
from simulator.StockLoader import StockLoader
sl = StockLoader.create()
stockdf = sl.loadStockDf()
factordf = sl.loadFactor()
# In[1]: 액면

import pandas as pd

priceLimitDate = pd.to_datetime('2015-06-15', format='%Y-%m-%d')
length = len(stockdf.index)
for index in stockdf.index:
    idx = stockdf.index.get_loc(index)
    if not isinstance(idx, (int, float, complex)) or idx >= length - 1 :
        break
    if priceLimitDate > index:
        targets = stockdf.iloc[idx][stockdf.iloc[idx] * 1.25 <= stockdf.iloc[idx+1]].index
        if len(targets) >= 0:
            ratio = (stockdf.iloc[idx+1] / stockdf.iloc[idx])[targets]
            stockdf.iloc[idx:][targets] = stockdf.iloc[idx:][targets] / ratio
    elif priceLimitDate <= index:
        targets = stockdf.iloc[idx][stockdf.iloc[idx] * 1.3 <= stockdf.iloc[idx+1]].index
        if len(targets) >= 0:
            ratio = (stockdf.iloc[idx+1] / stockdf.iloc[idx])[targets]
            stockdf.iloc[idx:][targets] = stockdf.iloc[idx:][targets] / ratio
   

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

sumdf = pd.Series((stockdf['2015-12-30'].values[0] - stockdf['2015-01-02'].values[0])/stockdf['2015-01-02'].values[0], index=stockdf['2015-12-30'].columns.values)
sumdf
# In[2]:
import numpy as np
import pandas as pd
# sumdf = muddf['2017-01-01':'2017-12-31'].sum().sort_values(ascending=False)
sumdf = sumdf.dropna()
for key in factordf.keys():
    factordf[key] = factordf[key].set_index(['종목명'])
factordf

# In[3]: 증가율
print(factordf.keys())
keys = list(factordf.keys())
for factorKey in keys:
    onedf = factordf[factorKey]
    compdf = onedf.shift(-1, axis=1)
    compdf['종목명'] = np.nan
    compdf['결산월'] = np.nan
    compdf['단위'] = np.nan
    targetdf = onedf - compdf
    # print(onedf)
    # targetdf['종목명'] = onedf['종목명']
    factordf[factorKey+'증가율'] = targetdf
print(factordf.keys())

# In[4]: 확인
print(factordf.keys())
factordf['영업활동으로인한현금흐름증가율']

# In[4]: 평균
print(factordf['roic'])
def getFactor(factor, ascending):
    return factordf[factor]['2014'].sort_values(ascending=ascending)

def getMean(start, end, sortfactordf):
    # meanLi = []
    # for name in sortfactordf.index:
    #     meanLi.append(sumdf.iloc[start:end].loc[name].mean())
    # print(sumdf[sortfactordf.iloc[start:end].index.values])
    # print(sumdf[sortfactordf.iloc[start:end].index.values].values)
    return sumdf[sortfactordf.iloc[start:end].index.values].values
    # return sumdf.iloc[start:end].loc[sortfactordf.index.values].mean()
def getFactorMean(factor, plotdf, ascending):
    factorValdf = getFactor(factor, ascending)
    meanList = np.array([])
    factorValList = []
    for idx in range(0, factorValdf.size, 30):
        meanList = np.hstack([meanList,getMean(idx, idx+30, factorValdf)])
        # meanList += getMean(idx, idx+30, factorValdf)
        # factorValList.append(factorValdf.iloc[idx:idx+30].mean())
    plotdf[factor] = meanList
    print(meanList)
    # plotdf[factor+'MEAN'] = factorValList
    return plotdf
plotdf = pd.DataFrame()
plotdf = getFactorMean('roic', plotdf, False)
plotdf = getFactorMean('pcr', plotdf)
plotdf = getFactorMean('roe', plotdf)
plotdf = getFactorMean('pbr', plotdf)
# plotdf['per+pcr'] = (plotdf['per'] + plotdf['pcr'])/2
# plotdf['per+pcr+pbr'] = (plotdf['per'] + plotdf['pcr']+ plotdf['pbr'])/2
plotdf = plotdf.dropna()
print(plotdf.plot(figsize = (18,12), fontsize=12))
plotdf
# In[3]: 수익률 당 팩터
# def factorMean(start, end, factor):
#     sumFactor = 0
#     countFactor = 0
#     for name in sumdf.iloc[start:end].index:
#         val = factordf[factor][factordf[factor]['종목명']==name][2014].values
#         if not np.isnan(val) and len(val)>0 and val[0]< 300:
#             sumFactor += val[0]
#             countFactor += 1
#             print(val[0])
#     if countFactor == 0:
#         return
#     print(factor,'Mean:', sumFactor/countFactor)
#     return sumFactor/countFactor
    

# def perMean(start, end, pltdf):
#     perMean = factorMean(start, end, 'per')
#     pcrMean = factorMean(start, end, 'pcr')
#     pbrMean = factorMean(start, end, 'pbr')
#     roeMean = factorMean(start, end, 'roe')
    
#     yieldMean = sumdf.iloc[start:end].mean()
#     print('yieldMean', sumdf.iloc[start:end].mean())
#     pltdf = pltdf.append(pd.Series([perMean,pcrMean, pbrMean,roeMean, yieldMean],index=['per', 'pcr', 'pbr', 'roe', 'yieldMean']), ignore_index=True)
#     return pltdf

# print(int(sumdf.size/30))
# pltdf = pd.DataFrame(columns=['per','pcr','pbr', 'roe'])
# for idx in range(0, sumdf.size, 30):
#     print('range:', idx,'~',idx+30)
#     pltdf = perMean(idx, idx+30, pltdf)
# pltdf = pltdf / pltdf.iloc[0]
# print(pltdf.plot(figsize = (18,12), fontsize=12))
# pltdf

# for idx in 
# A = namedf.loc['2015-01-01':'2019-06-06'].values.flatten()
# unique,pos = np.unique(A,return_inverse=True) #Finds all unique elements and their positions
# counts = np.bincount(pos)                     #Count the number of each unique element
# poss = counts.argsort()[::-1][:30]
# for name in list(map(lambda pos: unique[pos],poss)):
#     print(factordf['per'][factordf['per']['종목명']==name])
    # print(factordf['pcr'][factordf['pcr']['종목명']==name])
    


# (unique[maxpos],counts[maxpos])
# a = namedf.values.flatten()
# counts = np.bincount(a)
# print(np.argmax(counts))


#%%
