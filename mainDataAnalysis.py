# In[0]:
from simulator.StockLoader import StockLoader
sl = StockLoader.create()
stockdf = sl.loadStockDf()
factordf = sl.loadFactor()

# In[1]:
import pandas as pd
mdf = stockdf.resample('M').mean()
shiftdf = mdf.shift(1)
muddf = (mdf - shiftdf) / shiftdf
muddf
# valuedf = pd.DataFrame(columns=list(range(1,31)))
# namedf = pd.DataFrame(columns=list(range(1,31)))
# for idx in muddf.index:
#     sortdf = muddf.loc[idx].sort_values(ascending=False)
#     namedf.loc[idx] = sortdf.index
#     valuedf.loc[idx] = sortdf.values

# In[2]:
import numpy as np
import pandas as pd
sumdf = muddf['2015-01-01':'2015-12-31'].sum().sort_values(ascending=False)
print(sumdf)

def factorMean(start, end, factor):
    sumFactor = 0
    countFactor = 0
    for name in sumdf.iloc[start:end].index:
        val = factordf[factor][factordf[factor]['종목명']==name][2014].values
        if not np.isnan(val) and len(val)>0:
            sumFactor += val[0]
            countFactor += 1
    if countFactor == 0:
        return
    print(factor,'Mean:', sumFactor/countFactor)
    return sumFactor/countFactor
    

def perMean(start, end, pltdf):
    perMean = factorMean(start, end, 'per')
    pcrMean = factorMean(start, end, 'pcr')
    pbrMean = factorMean(start, end, 'pbr')
    roeMean = factorMean(start, end, 'roe')
    print('yieldMean', sumdf.iloc[start:end].mean())
    pltdf = pltdf.append(pd.Series([perMean,pcrMean, pbrMean,roeMean],index=['per', 'pcr', 'pbr', 'roe']), ignore_index=True)
    return pltdf

print(int(sumdf.size/30))
pltdf = pd.DataFrame(columns=['per','pcr','pbr', 'roe'])
for idx in range(0, sumdf.size, 30):
    print('range:', idx,'~',idx+30)
    pltdf = perMean(idx, idx+30, pltdf)
pltdf.plot()

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
