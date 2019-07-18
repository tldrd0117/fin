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
wallet = Wallet.create()
nextInvestDay = current
moneyOrder = []
losscutTarget = []
alreadyCut = []
buyDate = current


while endDate > current:
    if nextInvestDay <= current:
        wallet.clear()
        buyDate = current
        stockMoney = 0
        restMoney = money
        #한달마다 주식 변경
        nextInvestDay = current + pd.Timedelta(1, unit='M')
        target = list(topdf.columns)
        # target = ss.getMomentumList(current, topdf[target], mNum=2, mUnit='M', limit=1000, minVal=0)
        # target = ss.getRiseMeanList(current, topdf[target], 500, 0)
        target = ss.getFactorList(current, topdf[target], factordf, 'pcr', True, 50)
        target = ss.getFactorList(current, topdf[target], factordf, 'per', True, 30, minVal=0.000001)
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
        stock['money'] = latelyPrice
        

    #수익률에따른 계산
    for stock in wallet.getAllStock():
        stockMoney += stock['quantity'] * stock['money']

    nextDay = current + pd.Timedelta(1, unit='D')

    current = nextDay
    money = stockMoney + restMoney
    moneySum.loc[current] = money
    print(current, money, stockMoney, restMoney)
    # time.sleep(0.05)
# In[4]: look
moneySum
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
plt = jisuDf.plot(figsize = (18,12), fontsize=12)
fontProp = fm.FontProperties(fname=path, size=18)
plt.legend(prop=fontProp)
print(plt)

#%%

