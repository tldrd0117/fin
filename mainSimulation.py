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

current = pd.to_datetime('2008-05-01', format='%Y-%m-%d')
endDate = pd.to_datetime('2019-05-01', format='%Y-%m-%d')
money = 10000000
moneySum = pd.Series()
hold = []
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
    currentStr = current.strftime(format='%Y-%m-%d')
    nextStr = nextDate.strftime(format='%Y-%m-%d')
    for stockName in target:
        investMoney = stockMoney * moneyRate
        q = st.possibleQuantity(current, investMoney, stockName)
        so = StockOrder.create(stockName, q, investMoney)
        #일단사기
        results.append(st.buy(so, currentStr, nextStr))
        lossCutResult = st.losscut(so, currentStr, nextStr)
        if lossCutResult['isLosscut']:
            results.append(st.sell(so, lossCutResult['cutDate']))
    stockMoney = 0
    restMoney = 0
    for result in results:
        stockMoney += result['stock']
        restMoney += result['rest']
    money = stockMoney + restMoney
    current = nextDate
    moneySum.loc[current] = money
    print(current, money)

# In[3]: 통계
moneySum.index = moneySum.index.map(lambda dt: pd.to_datetime(dt.date()))
portfolio = moneySum / 10000000
투자기간 = len(moneySum.index)/12
print(투자기간)
# print(portfolio)
e = pd.date_range(start=portfolio.index[12], periods=투자기간, freq=pd.DateOffset(years=1))
d = [ portfolio.index.get_loc(date, method='nearest')for date in e]
print(portfolio[d]/portfolio[d].shift(1))
# print((portfolio[d]**(1/12))*100-100)
print((portfolio/portfolio.shift(1)).sum()/len(portfolio.index))
print(portfolio[-1]/portfolio.std())
print((portfolio/portfolio.shift(1)))

print('연평균 수익률',((portfolio[-1]**(1/투자기간))*100-100))

print('최대 하락률',((portfolio - portfolio.shift(1))/portfolio.shift(1)*100).min())
print('최대 상승률',((portfolio - portfolio.shift(1))/portfolio.shift(1)*100).max())

#%%
