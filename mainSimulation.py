# coding=UTF-8
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
import logging
logging.basicConfig(handlers=[logging.FileHandler('simulation6.log', 'w', 'utf-8')], level=logging.INFO, format='%(message)s')
pd.set_option('display.float_format', None)
np.set_printoptions(suppress=True)
def printG(*msg):
    joint = ' '.join(list(map(lambda x : str(x), msg)))
    print(joint)
    logging.info(joint)


# In[1]:
#StockLoader 시간에따른 주식 가격을 가져온다

sl = StockLoader.create()
topdf, topcap, sCode, sName = sl.loadTopDf()
topdf2 = sl.loadTopLately(topcap, '2019-05-01','2019-08-04')
topdf3 = sl.loadTopLately(topcap, '2019-08-05','2019-08-05')
topdf4 = sl.loadTopLately(topcap, '2019-08-06','2019-08-08')
topdf5 = sl.loadTopLately(topcap, '2019-08-09','2019-08-09')
topdf6 = sl.loadTopLately(topcap, '2019-08-10','2019-08-16')
topdf7 = sl.loadTopLately(topcap, '2019-08-17','2019-08-22')
topdf = pd.concat([topdf, topdf2, topdf3, topdf4, topdf5, topdf6, topdf7])
topdf = topdf[~topdf.index.duplicated(keep='first')]
marcapdf = sl.loadMarcap()
factordf = sl.loadFactor()
qfactordf = sl.loadQuaterFactor()
intersect = list(set(topdf.columns) & set(topcap['Name'].values)) #+ ['KOSEF 국고채10년레버리지']
topdf = topdf[intersect]
# salesdf = factordf['영업활동으로인한현금흐름']
# compdf = salesdf.shift(-1, axis=1)
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
one = None
topdf
# In[232]:stockNum
# In[222]:
epsdf = factordf['eps']
compdf = epsdf.shift(-1, axis=1)
compdf['종목명'] = np.nan
compdf['결산월'] = np.nan
compdf['단위'] = np.nan
compdf[2007] = -1
# compdf = compdf.fillna(-1)
print(compdf)
targetdf = epsdf-compdf
targetdf['종목명'] = epsdf['종목명']
factordf['eps증가율'] = targetdf

factordf['eps증가율']

# In[232]:
def isNumber(val):
    return isinstance(val, (int, float, complex))
def isnum(s):
    try:
        float(s)
    except:
        return(False)
    else:
        return(True)
def toNumeric(df):
    return df.applymap(lambda v : float(v) if isNumber(v) or isnum(v) else np.nan )
if not one:
    for key in factordf.keys():
        factordf[key].columns = list(map(lambda col : float(col) if isNumber(col) or col.isnumeric() else col, factordf[key].columns))
    # for key in factordf.keys():
        # factordf[key] = factordf[key].set_index(['종목명'])
    for key in factordf.keys():
        factordf[key] = toNumeric(factordf[key])
one = True
# In[3]:
# In[2]:
ss = StockStrategy.create()
st = StockTransaction.create(topdf)

current = pd.to_datetime('2008-05-01', format='%Y-%m-%d')
endDate = pd.to_datetime('2019-08-22', format='%Y-%m-%d')
priceLimitDate = pd.to_datetime('2015-06-15', format='%Y-%m-%d')
money = 10000000
moneySum = pd.Series()
wallet = Wallet.create()
nextInvestDay = current
beforeInvestDay = current
nextYearDay = current
moneyOrder = []
losscutTarget = []
alreadyCut = []
buyDate = current

parValues = []
for val1 in [1,2,5,10,20,25,50,100]:
    for val2 in [1,2,5,10,20,25,50,100]:
        parValues.append(val1/val2)
parValues = list(set(parValues))

allfactors = ['per', 'pcr', 'pbr', 'roe', '당기순이익', '영업활동으로인한현금흐름', '투자활동으로인한현금흐름', '재무활동으로인한현금흐름', 'psr', 'roic', 'eps', 'ebit', 'ev_ebit', 'ev_sales', 'ev_ebitda', '당기순이익률', '영업이익률', '매출총이익률', '배당수익률', '매출액', '자산', '유동자산', '부채','유동부채']
# weights = {'per':0.30458087, 'pcr':-0.03745455, 'pbr':0.23468399, 'roe':0.36092985, '당기순이익':0.19461265, '영업활동으로인한현금흐름':0.05416971, '투자활동으로인한현금흐름':0.3000804, '재무활동으로인한현금흐름':0.0256378, 'psr':-1.3246877, 'roic':-0.35830158, 'eps':0.02405762, 'ebit':0.04227263, 'ev_ebit':0.0967356, 'ev_sales':0.01554028, 'ev_ebitda':0.5191966, '당기순이익률':0.13129355, '영업이익률':0.15036112, '매출총이익률':0.44065595, '배당수익률':0.52419686, '매출액':-0.69880736}
# weights = {'per': 0.05186881, 'pcr':0.03852479, 'pbr':0.0313778, 'roe':0.03718218, '당기순이익':-0.00151721, '영업활동으로인한현금흐름': 0.01412872, '투자활동으로인한현금흐름':0.07871272, '재무활동으로인한현금흐름':0.02713266, 'psr':0.01447739, 'roic':0.03316823, 'eps':0.00521486, 'ebit':0.01497366, 'ev_ebit':0.07060508, 'ev_sales':0.00256039, 'ev_ebitda':0.04957749, '당기순이익률':0.03347141, '영업이익률':0.0247674, '매출총이익률':0.03217566, '배당수익률':0.09379209, '매출액':0.01340407}
# weights = {'per':0.30458087, 'pcr':-0.03745455, 'pbr':0.23468399, 'roe':0.36092985, '당기순이익':0.19461265, '영업활동으로인한현금흐름':0.05416971, '투자활동으로인한현금흐름':0.3000804, '재무활동으로인한현금흐름':0.0256378, 'psr':-1.3246877, 'roic':-0.35830158, 'eps':0.02405762, 'ebit':0.04227263, 'ev_ebit':0.0967356, 'ev_sales':0.01554028, 'ev_ebitda':0.5191966, '당기순이익률':0.13129355, '영업이익률':0.15036112, '매출총이익률':0.44065595, '배당수익률':0.52419686, '매출액':-0.69880736}
# weights = {'per': 0.03739188, 'pcr':0.02552654, 'pbr':-0.04820556, 'roe':-0.05617258, 'psr':0.03603807, 'roic':-0.06701323, 'eps':-0.07390497, 'ebit':0.0807128, 'ev_ebit':0.03940655, 'ev_sales':-0.100779, 'ev_ebitda':-0.03655547, '당기순이익률':-0.02207778, '영업이익률':-0.05997825, '매출총이익률':0.08456226, '배당수익률':-0.04298059}
factors = ['per', 'pcr', 'pbr', 'roe', 'psr', 'roic', 'eps', 'ebit', 'ev_ebit', 'ev_sales', 'ev_ebitda', '당기순이익률', '영업이익률', '매출총이익률', '배당수익률']
weights = {'per': 1.63437670e+05, 'pcr':-1.51993291e+05, 'pbr':3.01448559e+05, 'roe':3.33162663e+03, 'psr':0, 'roic':0, 'eps':0, 'ebit':-4.57452298e+04, 'ev_ebit':0, 'ev_sales':-2.20027105e+01, 'ev_ebitda':1.24530671e+04, '당기순이익률':0, '영업이익률':0, '매출총이익률':-9.64486095e+04, '배당수익률':-4.84420386e+04}

investigation = []
rebalanceRate = 0
momentumList = []
konexCode = ['076340','223220','186230','112190','214610','224880','086220','284610','272420','178600','114920','199150','183410','176750','220110','086080','200350','271850','302920','270020','163430','216400','126340','232680','285770','215050','110660','135270','162120','210610','302550','185190','217320','233250','103660','189330','189540','267060','279600','149010','281310','208890','158300','086460','266170','203400','064850','260870','236030','271400','179720','216280','140660','267810','270660','121060','084440','221800','210120','101360','270210','299670','219750','140290','217910','189350','136660','116100','262760','199800','217880','202960','276240','225860','065370','107640','222670','121060','224760','225220','221670','299480','220250','207230','224810','228180','229000','229500','092590','217950','211050','230400','180060','225850','044990','232680','226610','258050','234070','277880','266870','278990','135160','215570','233990','176560','232530','206950','067370','236340','237720','222160','284420','238500','240340','241510','058970','242420','066830','228760','242850','112190','244880','245030','229480','208850','245450','246250','247300','224020','212310','191600','250300','239890','251960','252370','251280','243870','253840','199290','148780','167380','258540','232830','258250','227420','260970','205290','242350','120780']
defblackList = list(map(lambda x : sCode[x] if x in sCode.keys() else '', konexCode))
blackList = defblackList

while endDate > current:
    if nextYearDay <= current:
        nextYearDay = current + pd.Timedelta(1, unit='Y')
        if len(momentumList) > 0:
            printG('momentumSumTotal',sum(momentumList), sum(momentumList)/len(momentumList))
        momentumList = []
        blackList = defblackList

    if nextInvestDay <= current:
        #표시하기
        for pair in investigation:
            # if pair['code'] in alreadyCut:
            #     continue
            lastMoney = wallet.getStockLastMoney(pair['code'])
            if not lastMoney:
                printG(pair['code'], 'notExist')
                continue
            ratio = wallet.getStockRatio(pair['code'])
            printG(lastMoney, pair['price'], ratio, pair['price']*ratio)
            printG('####################################')
            printG(pair['code'], lastMoney/(pair['price']*ratio))
            # codedf = marcapdf[marcapdf['Code']==sName[pair['code']]]
            # codedf = codedf.loc[~codedf.index.duplicated(keep='first')]
            # beforeLoc = codedf.index.get_loc(beforeInvestDay, method='ffill')
        
            # printG('거래대금:',codedf.iloc[beforeLoc]['Amount'])
            printG('####################################')
            for factor in allfactors:
                printG(factor,':',ss.getFactor(current, factordf, factor, pair['code'], sName))
            # if lastMoney/(pair['price']*ratio) <= 0.8:
                # blackList.append(pair['code'])
        # printG('blackList', blackList)

        # #한달 수익률
        if current != buyDate:
            afterBuyDate = buyDate + pd.Timedelta(1, unit='d')
            printG('monthPercentage: ', (moneySum.loc[current] - moneySum.loc[afterBuyDate])/moneySum.loc[afterBuyDate] * 100 )
        
        investigation = []
        wallet.clear()
        buyDate = current
        stockMoney = 0
        restMoney = int(money * (1 - rebalanceRate))
        rebalaceMoney = int(money * rebalanceRate)
        #한달마다 주식 변경
        # nextInvestDay = current + pd.Timedelta(1, unit='M')
        #15일마다 주식 변경
        # nextInvestDay = current + pd.Timedelta(15, unit='d')
        #7일마다 주식 변경
        beforeInvestDay = current
        nextInvestDay = current + pd.Timedelta(1, unit='M')
        target = list(topdf.columns)
        if len(blackList) > 0:
            target = list(filter(lambda x : x not in blackList, target ))

        target = ss.filterAltmanZScore(current, topdf[target], factordf, topcap, sName, sCode )
        printG('altman', len(target))
        # target = ss.getAmount(current, marcapdf, sCode, limit=0)
        inter = list(set(topdf.columns) & set(target))
        target = ss.getFactorList(current, topdf[inter], factordf, 'roe', sName, sCode, False, 3000, minVal=0.00000001)
        # target = ss.getFactorList(current, topdf[target], factordf, 'eps증가율', sName, sCode, False, 3000, minVal=0)
        target = ss.getFactorList(current, topdf[target], factordf, '영업이익률', sName, sCode, False, 3000, minVal=0.00000001)
        target = ss.getFactorList(current, topdf[target], factordf, '당기순이익률', sName, sCode, False, 3000, minVal=3)
        # target = ss.getCurValuePerStockNumFactor(current, topdf[target], factordf, '당기순이익', marcapdf, sCode, sName, 1000, True, int(len(target)/2), minVal=0.00000001)
        target = ss.getCurValuePerStockNumFactor(current, topdf[target], factordf, '당기순이익', marcapdf, sCode, sName, 1000, True, int(len(target)/2), minVal=0.00000001)
        target = ss.getCurValuePerStockNumFactor(current, topdf[target], factordf, '영업활동으로인한현금흐름', marcapdf, sCode, sName, 1000, True, 50, minVal=0.00000001)
        beforebeforeTarget = target
        target = ss.getFactorList(current, topdf[target], factordf, '당기순이익률', sName, sCode, True, 30, minVal=3)
        target = ss.getFactorPerStockNum(current, topdf[target], factordf, '영업활동으로인한현금흐름', marcapdf, sCode, sName, False, 30, minVal=0.00000001)
        target = ss.getFactorList(current, topdf[target], factordf, '영업활동으로인한현금흐름',sName, sCode, False, 30, minVal=0.00000001)
        # target = ss.getFactorList(current, topdf[target], factordf, 'eps', False, 30, minVal=0)
        # target, momentumSum = ss.getMomentumList(current, topdf[target], mNum=24, mUnit='M', limit=30, minVal=0.00000001)
        beforeTarget = target
        notMomentumTarget = target
        if len(target) > 0:
            target, momentumSum = ss.getMomentumList(current, topdf[target], mNum=12, mUnit='M', limit=30, minVal=0.00000001)
        only12MomentumTarget = target
        if len(target) > 0:
            target, momentumSum = ss.getMomentumList(current, topdf[target], mNum=2, mUnit='M', limit=30, minVal=0.00000001)
        printG('momentumSum', momentumSum)
        # if current.month >=4 and current.month <=10:
        #     target = []
        # if len(target) < 2:
        #     target = []
        #     target = ss.getFactorList(current, topdf[beforebeforeTarget], factordf, '부채', sName, sCode, True, 10, minVal=0.00000001)

        # if ss.isUnemployedYear(current.year) :
        #     # printG('isUnemployedYear')
        #     target = beforeTarget

        # if abs(momentumSum) <= 5:
            # target = beforeTarget
        # momentumList.append(momentumSum)
        # if momentumSum <= 5:
            # target = beforeTarget
        # target = ss.getFactorList(current, topdf[target], factordf, 'ev_ebitda', True, 30)
        # target = ss.getFactorList(current, topdf[target], factordf, 'eps', False, int(len(target)*4/5))
        # target = ss.getFactorList(current, topdf[target], factordf, 'per', False, int(len(target)*4/5))
        # target = ss.getFactorList(current, topdf[target], factordf, 'pbr', False, 30)
        # target = ss.getFactorList(current, topdf[target], factordf, '영업활동으로인한현금흐름', False, 30)
        # target = ss.getFactorList(current, topdf[target], factordf, 'pcr', True, 50)
        # target = ss.getFactorList(current, topdf[target], factordf, 'per', True, 30)
        # target = ss.getFactorList(current, topdf[target], factordf, 'psr', True, 1)
        # target = ss.getFactorList(current, topdf[target], factordf, '당기순이익', True, 200)
        # target = ss.getFactorList(current, topdf[target], factordf, '영업활동으로인한현금흐름', False, 30, minVal=0.000001)
        # target = ss.getRsi30perList(current, topdf[target], 30)
        printG('NOT_MOMENTUM_TARGET@@@@@@@@@@@@@@@@@@@@@@@@@')
        printG('investNum1:',len(notMomentumTarget))
        printG(notMomentumTarget)
        printG('12_MOMENTUM_TARGET@@@@@@@@@@@@@@@@@@@@@@@@@@')
        printG('investNum2:',len(only12MomentumTarget))
        printG(only12MomentumTarget)
        printG('INVEST_TARGET@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
        printG('investNum3:',len(target))
        printG(target)
        if len(target) <= 0:
            moneyRate = 0
        else:
            moneyRate = 1 / len(target)
        results = []
        currentStr = current.strftime(format='%Y-%m-%d')
        nextStr = nextInvestDay.strftime(format='%Y-%m-%d')

        readyMoney = restMoney * 1
        # if ss.isUnemployedYear(current.year):
        #     readyMoney = restMoney * 0.75
        # else:
        #     readyMoney = restMoney * 1

        for stockName in target:
            investigation.append({'code':stockName, 'price':st.getValue(current, stockName)})
            investMoney = readyMoney * moneyRate
            q = st.possibleQuantity(current, investMoney, stockName)
            if not q:
                continue
            so = StockOrder.create(stockName, q, investMoney)
            #일단사기
            buyMoney = st.getValue(current, stockName)
            
            wallet.buy(so.code, so.quantity, buyMoney)
            restMoney -= buyMoney * so.quantity

        #채권
        # bondName = 'KOSEF 국고채10년레버리지'
        # q=st.possibleQuantity(current, restMoney, bondName)
        # if q:
        #     investigation.append({'code':bondName, 'price':st.getValue(current, bondName)})
        #     printG('채권:', q, '개 매매')
        #     buyMoney = st.getValue(current, bondName)
        #     wallet.buy(bondName, q, buyMoney)

        #     restMoney -= buyMoney * q

        losscutTarget = []
        alreadyCut = []
        for stock in wallet.getAllStock():
            loss = st.calculateLosscutRate(stock['code'], current)
            printG(stock['code'], loss)

    # #blacklist
    # for stock in wallet.getAllStock():
    #     isLosscut = st.losscutScalar(stock['code'], current, buyDate, 0.75)
    #     if isLosscut:
    #         blackList.append(stock['code'])
    #         printG('blackList', blackList)
    #인버스
    # for stock in wallet.getAllStock():
    #     isLosscut = st.losscut(stock['code'], current, buyDate)
    #     if isLosscut and stock['code'] not in losscutTarget:
    #         losscutTarget.append(stock['code'])
    #         if len(losscutTarget) >= 15: 
    #             printG('물타기갯수:', len(losscutTarget))
    #             for lossTarget in losscutTarget:
    #                 if lossTarget not in alreadyCut:
    #                     bondName = lossTarget
    #                     sailingMoney = rebalaceMoney/15
    #                     q=st.possibleQuantity(current, sailingMoney, bondName)
    #                     if q:
    #                         alreadyCut.append(lossTarget)
    #                         investigation.append({'code':bondName, 'price':st.getValue(current, bondName)})
    #                         printG('물타기 '+lossTarget+':', q, '개 매매')
    #                         buyMoney = st.getValue(current, bondName)
    #                         wallet.buy(bondName, q, buyMoney)
    #                         rebalaceMoney -= buyMoney * q
    #손절 Sum
    stockCodes = list(map(lambda x : x['code'], wallet.getAllStock()))
    isLosscut = st.losscutScalarSum(stockCodes, current, buyDate, 0.9)
    if isLosscut:
        for stock in wallet.getAllStock():
            if stock['code'] not in losscutTarget:
                losscutTarget.append(stock['code'])
                if len(losscutTarget) >= len(target):
                    printG('손절갯수:', len(losscutTarget))
                    for lossTarget in losscutTarget:
                        if lossTarget not in alreadyCut:
                            alreadyCut.append(lossTarget)
                            lossStock = wallet.getStock(lossTarget)
                            stockQuantity = lossStock['quantity']
                            sellMoney = st.getValue(current, lossTarget)
                            isSold = wallet.sell(lossStock['code'], stockQuantity, sellMoney)
                            if isSold:
                                restMoney += sellMoney * stockQuantity
                            

    #손절
    # for stock in wallet.getAllStock():
    #     # isLosscut = st.losscut(stock['code'], current, buyDate)
    #     isLosscutScalar = st.losscutScalar(stock['code'], current, buyDate, 0.9)
    #     # if isLosscutScalar and stock['code'] not in blackList:
    #     #     blackList.append(stock['code'])
    #     if isLosscutScalar and stock['code'] not in losscutTarget:
    #         losscutTarget.append(stock['code'])
    #         if len(losscutTarget) >= len(target):
    #             printG('손절갯수:', len(losscutTarget))
    #             for lossTarget in losscutTarget:
    #                 if lossTarget not in alreadyCut:
    #                     alreadyCut.append(lossTarget)
    #                     lossStock = wallet.getStock(lossTarget)
    #                     stockQuantity = lossStock['quantity']
    #                     sellMoney = st.getValue(current, lossTarget)
    #                     isSold = wallet.sell(lossStock['code'], stockQuantity, sellMoney)
    #                     if isSold:
    #                         restMoney += sellMoney * stockQuantity
                            
                            #채권 사기
                            # bondName = 'KOSEF 국고채10년레버리지'
                            # q=st.possibleQuantity(current, restMoney, bondName)
                            # if q:
                            #     printG('채권:', q, '개 매매')
                            #     buyMoney = st.getValue(current, bondName)
                            #     wallet.buy(bondName, q, buyMoney)
                            #     restMoney -= buyMoney * q


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
            stock['ratio'] = ratio

        stock['money'] = latelyPrice
        

    #수익률에따른 계산
    for stock in wallet.getAllStock():
        stockMoney += stock['quantity'] * stock['money']

    nextDay = current + pd.Timedelta(1, unit='D')

    current = nextDay
    money = stockMoney + restMoney + rebalaceMoney
    moneySum.loc[current] = money
    printG(current, money, stockMoney, restMoney, rebalaceMoney)
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
printG(portfolio[d]/portfolio[d].shift(1))
# print((portfolio[d]**(1/12))*100-100)
# print((portfolio/portfolio.shift(1)).sum()/len(portfolio.index))
# print(portfolio[-1]/portfolio.std())
# print((portfolio/portfolio.shift(1)))

m = pd.date_range(start=portfolio.index[0],end=portfolio.index[-1] + pd.Timedelta(1, unit='D'), freq='M')

printG('연평균 수익률',((portfolio[-1]**(1/(2019-2008)))*100-100))

printG('최대 하락률',((portfolio[m] - portfolio[m].shift(1))/portfolio[m].shift(1)*100).min())
printG('최대 상승률',((portfolio[m] - portfolio[m].shift(1))/portfolio[m].shift(1)*100).max())

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
printG(jisuDf)
plot = jisuDf.plot(figsize = (18,12), fontsize=12)
fontProp = fm.FontProperties(fname=path, size=18)
plot.legend(prop=fontProp)
plt.show()

#%%

