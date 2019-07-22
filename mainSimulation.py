# In[0]:
from simulator.Shares import Shares
from simulator.StockLoader import StockLoader
from simulator.StockTransaction import StockTransaction
from simulator.StockOrder import StockOrder
from simulator.StockStrategy import StockStrategy
from simulator.Wallet import Wallet
import pandas as pd
import asyncio
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import numpy as np


# In[1]:
#StockLoader 시간에따른 주식 가격을 가져온다

sl = StockLoader.create()
topdf = sl.loadTopDf()
factordf = sl.loadFactor()


# salesdf = factordf['영업활동으로인한현금흐름']
# compdf = salesdf.shift(-1, axis=1)
# compdf['종목명'] = np.nan
# compdf['결산월'] = np.nan
# compdf['단위'] = np.nan
# targetdf = salesdf-compdf
# targetdf['종목명'] = salesdf['종목명']
# factordf['영업활동으로인한현금흐름증가율'] = targetdf

# yielddf = factordf['당기순이익']
# compdf = yielddf.shift(-1, axis=1)
# compdf['종목명'] = np.nan
# compdf['결산월'] = np.nan
# compdf['단위'] = np.nan
# targetdf = yielddf-compdf
# targetdf['종목명'] = yielddf['종목명']
# factordf['당기순이익증가율'] = targetdf
# In[232]:
def isNumber(val):
    return isinstance(val, (int, float, complex)) 
for key in factordf.keys():
    factordf[key].columns = list(map(lambda col : float(col) if isNumber(col) or col.isnumeric() else col, factordf[key].columns))
for key in factordf.keys():
    factordf[key] = factordf[key].set_index(['종목명'])
print(factordf)
# In[2]:
ss = StockStrategy.create()
st = StockTransaction.create(topdf)

current = pd.to_datetime('2008-05-01', format='%Y-%m-%d')
endDate = pd.to_datetime('2019-05-01', format='%Y-%m-%d')
priceLimitDate = pd.to_datetime('2015-06-15', format='%Y-%m-%d')
money = 10000000
moneySum = pd.Series()
wallet = Wallet.create()
nextInvestDay = current
moneyOrder = []
losscutTarget = []
alreadyCut = []
buyDate = current

parValues = []
for val1 in [1,2,5,10,20,25,50,100]:
    for val2 in [1,2,5,10,20,25,50,100]:
        parValues.append(val1/val2)
parValues = list(set(parValues))

factors = ['per', 'pcr', 'pbr', 'roe', '당기순이익', '영업활동으로인한현금흐름', '투자활동으로인한현금흐름', '재무활동으로인한현금흐름', 'psr', 'roic', 'eps', 'ebit', 'ev_ebit', 'ev_sales', 'ev_ebitda', '당기순이익률', '영업이익률', '매출총이익률', '배당수익률', '매출액']
# weights = {'per':0.30458087, 'pcr':-0.03745455, 'pbr':0.23468399, 'roe':0.36092985, '당기순이익':0.19461265, '영업활동으로인한현금흐름':0.05416971, '투자활동으로인한현금흐름':0.3000804, '재무활동으로인한현금흐름':0.0256378, 'psr':-1.3246877, 'roic':-0.35830158, 'eps':0.02405762, 'ebit':0.04227263, 'ev_ebit':0.0967356, 'ev_sales':0.01554028, 'ev_ebitda':0.5191966, '당기순이익률':0.13129355, '영업이익률':0.15036112, '매출총이익률':0.44065595, '배당수익률':0.52419686, '매출액':-0.69880736}
weights = {'per': 0.05186881, 'pcr':0.03852479, 'pbr':0.0313778, 'roe':0.03718218, '당기순이익':-0.00151721, '영업활동으로인한현금흐름': 0.01412872, '투자활동으로인한현금흐름':0.07871272, '재무활동으로인한현금흐름':0.02713266, 'psr':0.01447739, 'roic':0.03316823, 'eps':0.00521486, 'ebit':0.01497366, 'ev_ebit':0.07060508, 'ev_sales':0.00256039, 'ev_ebitda':0.04957749, '당기순이익률':0.03347141, '영업이익률':0.0247674, '매출총이익률':0.03217566, '배당수익률':0.09379209, '매출액':0.01340407}
while endDate > current:
    if nextInvestDay <= current:
        wallet.clear()
        buyDate = current
        stockMoney = 0
        restMoney = money
        #한달마다 주식 변경
        nextInvestDay = current + pd.Timedelta(1, unit='M')
        target = list(topdf.columns)
        target = ss.getFactorLists(current, topdf[target], factordf, factors, 30, weights)
        # target = ss.getMomentumList(current, topdf[target], mNum=2, mUnit='M', limit=1000, minVal=0)
        # target = ss.getRiseMeanList(current, topdf[target], 500, 0)
        # target = ss.getFactorList(current, topdf[target], factordf, 'per', True, 50)
        # target = ss.getFactorList(current, topdf[target], factordf, 'pcr', True, 30)
        # target = ss.getFactorList(current, topdf[target], factordf, '당기순이익', True, 200)
        # target = ss.getFactorList(current, topdf[target], factordf, '투자활동으로인한현금흐름', False, 30)
        # target = ss.getFactorList(current, topdf[target], factordf, '영업활동으로인한현금흐름', False, 30, minVal=0.000001)
        # target = ss.getRsi30perList(current, topdf[target], 30)

        print(target)
        moneyRate = 1 / 30
        results = []
        currentStr = current.strftime(format='%Y-%m-%d')
        nextStr = nextInvestDay.strftime(format='%Y-%m-%d')
        for stockName in target:
            investMoney = money * moneyRate
            q = st.possibleQuantity(current, investMoney, stockName)
            if not q:
                continue
            so = StockOrder.create(stockName, q, investMoney)
            #일단사기
            buyMoney = st.getValue(current, stockName)
            wallet.buy(so.code, so.quantity, buyMoney)
            restMoney -= buyMoney * so.quantity

        #채권
        bondName = 'KOSEF 국고채10년레버리지'
        q=st.possibleQuantity(current, restMoney, bondName)
        if q:
            print('채권:', q, '개 매매')
            buyMoney = st.getValue(current, bondName)
            wallet.buy(bondName, q, buyMoney)
            restMoney -= buyMoney * q

        losscutTarget = []
        alreadyCut = []
    #손절
    
    for stock in wallet.getAllStock():
        isLosscut = st.losscut(stock['code'], current, buyDate)
        if isLosscut and stock['code'] not in losscutTarget:
            losscutTarget.append(stock['code'])
            if len(losscutTarget) >= 15:
                print('손절갯수:', len(losscutTarget))
                for lossTarget in losscutTarget:
                    if lossTarget not in alreadyCut:
                        alreadyCut.append(lossTarget)
                        lossStock = wallet.getStock(lossTarget)
                        stockQuantity = lossStock['quantity']
                        sellMoney = st.getValue(current, lossTarget)
                        isSold = wallet.sell(lossStock['code'], stockQuantity, sellMoney)
                        if isSold:
                            restMoney += sellMoney * stockQuantity
                            
                            #채권 사기
                            bondName = 'KOSEF 국고채10년레버리지'
                            q=st.possibleQuantity(current, restMoney, bondName)
                            if q:
                                print('채권:', q, '개 매매')
                                buyMoney = st.getValue(current, bondName)
                                wallet.buy(bondName, q, buyMoney)
                                restMoney -= buyMoney * q
    # for losscutTarget in st.losscutRsi(wallet.getAllStock(), current, topdf[target]):
    #     if losscutTarget['code'] in alreadyCut:
    #         continue
    #     print('RSI손절:', losscutTarget['code'], losscutTarget['quantity'])
    #     alreadyCut.append(losscutTarget['code'])
    #     stockQuantity = losscutTarget['quantity']
    #     sellMoney = st.getValue(current, losscutTarget['code'])
    #     isSold = wallet.sell(losscutTarget['code'], stockQuantity, sellMoney)
    #     if isSold:
    #         restMoney += sellMoney * stockQuantity
            
    #         #채권 사기
    #         bondName = 'KOSEF 국고채10년레버리지'
    #         q=st.possibleQuantity(current, restMoney, bondName)
    #         if q:
    #             print('채권:', q, '개 매매')
    #             buyMoney = st.getValue(current, bondName)
    #             wallet.buy(bondName, q, buyMoney)
    #             restMoney -= buyMoney * q
    
    #수익률 반영
    stockMoney = 0
    for stock in wallet.getAllStock():
        latelyPrice = st.getValue(current, stock['code'])
        if (current < priceLimitDate and stock['money'] * 1.25 <= latelyPrice or stock['money'] * 0.75 >= latelyPrice)\
            or (current >= priceLimitDate and stock['money'] * 1.3 < latelyPrice or stock['money'] * 0.7 > latelyPrice):
            ratio = latelyPrice / stock['money']
            idx = np.argmin(np.abs(parValues - ratio))
            stock['quantity'] = int(stock['quantity']/parValues[idx])

        stock['money'] = latelyPrice
        

    #수익률에따른 계산
    for stock in wallet.getAllStock():
        stockMoney += stock['quantity'] * stock['money']

    nextDay = current + pd.Timedelta(1, unit='D')

    current = nextDay
    money = stockMoney + restMoney
    moneySum.loc[current] = money
    print(current, money, stockMoney, restMoney)
# In[4]: look
# moneySum
# In[3]: 통계
moneySum.index = moneySum.index.map(lambda dt: pd.to_datetime(dt.date()))
portfolio = moneySum / 10000000

# 투자기간 = len(moneySum.index)/12
# print(투자기간)
# print(portfolio)
투자기간 = portfolio.index[-1].year - portfolio.index[0].year
e = pd.date_range(start=portfolio.index[0],end=portfolio.index[-1] + pd.Timedelta(1, unit='D'), freq=pd.DateOffset(years=1))
d = [ portfolio.index.get_loc(date, method='nearest')for date in e]
print(portfolio[d]/portfolio[d].shift(1))
# print((portfolio[d]**(1/12))*100-100)
# print((portfolio/portfolio.shift(1)).sum()/len(portfolio.index))
# print(portfolio[-1]/portfolio.std())
# print((portfolio/portfolio.shift(1)))

m = pd.date_range(start=portfolio.index[0],end=portfolio.index[-1] + pd.Timedelta(1, unit='D'), freq='M')

print('연평균 수익률',((portfolio[-1]**(1/(2019-2008)))*100-100))

print('최대 하락률',((portfolio[m] - portfolio[m].shift(1))/portfolio[m].shift(1)*100).min())
print('최대 상승률',((portfolio[m] - portfolio[m].shift(1))/portfolio[m].shift(1)*100).max())

# In[4]: 그래프
import matplotlib.font_manager as fm
import platform
import matplotlib.pyplot as plt
if platform.system()=='Darwin':
    path = '/Library/Fonts/NanumBarunGothicLight.otf'
else:
    path = 'C:/Windows/Fonts/malgun.ttf'
choosedDf = moneySum
# choosedDf['KOSPI'] = kospidf['종가']
# choosedDf[bonddf.columns[0]] = bonddf[bonddf.columns[0]]
choosedDf = choosedDf.fillna(method='bfill').fillna(method='ffill')
# print(choosedDf)
jisuDf = choosedDf / choosedDf.iloc[0]
print(jisuDf)
plot = jisuDf.plot(figsize = (18,12), fontsize=12)
fontProp = fm.FontProperties(fname=path, size=18)
plot.legend(prop=fontProp)
plt.show()

#%%

