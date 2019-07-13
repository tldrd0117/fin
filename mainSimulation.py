# In[0]:
from simulator.Shares import Shares
from simulator.StockLoader import StockLoader
from simulator.StockTransaction import StockTransaction
from simulator.StockOrder import StockOrder
from simulator.StockStrategy import StockStrategy
import pandas as pd


# In[1]:
#StockLoader 시간에따른 주식 가격을 가져온다
sl = StockLoader.create()
topdf = sl.loadTopDf()
factordf = sl.loadFactor()


# In[2]:
ss = StockStrategy.create()
st = StockTransaction.create(topdf)

current = pd.to_datetime('2011-05-01', format='%Y-%m-%d')
endDate = pd.to_datetime('2012-05-01', format='%Y-%m-%d')
money = 1000000
while endDate > current:
    stockMoney = money
    restMoney = 0
    target = list(topdf.columns)
    target = ss.getMomentumList(current, topdf[target], mNum=2, mUnit='M', limit=1000, minVal=0)
    target = ss.getFactorList(current, topdf[target], factordf, 'pcr', True, 50)
    target = ss.getFactorList(current, topdf[target], factordf, 'per', True, 30, minVal=0.000001)
    moneyRate = 1 / 30
    nextDate = current + pd.Timedelta(1, unit='M')
    results = []
    for stockName in target:
        investMoney = stockMoney * moneyRate
        q = st.possibleQuantity(current, investMoney, stockName)
        so = StockOrder.create(stockName, q, investMoney)
        currentStr = current.strftime(format='%Y-%m-%d')
        nextStr = nextDate.strftime(format='%Y-%m-%d')
        results.append(st.buy(so,currentStr, nextStr))
    stockMoney = 0
    restMoney = 0
    for result in results:
        stockMoney += result['stock']
        restMoney += result['rest']
        print(result['stock'])
        print(result['rest'])
    money = stockMoney + restMoney
    current = nextDate
    print('################',money)

#%%
