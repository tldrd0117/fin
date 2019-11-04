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
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import numpy as np
import logging
logging.basicConfig(handlers=[logging.FileHandler('simulation18.log', 'w', 'utf-8')], level=logging.INFO, format='%(message)s')
pd.set_option('display.float_format', None)
np.set_printoptions(suppress=True)
def printG(*msg):
    joint = ' '.join(list(map(lambda x : str(x), msg)))
    print(joint)
    logging.info(joint)

# In[1]:
#StockLoader 시간에따른 주식 가격을 가져온다

sl = StockLoader.create()
topcap, sCode, sName = sl.loadTopcapDf()
topdfItems = []
amountdfItems = []
for fileName in os.listdir('h5data'):
    if fileName.startswith('STOCK_CLOSE'):
        topdfItems.append(pd.read_hdf('h5data/'+fileName, key='df'))
        print(fileName)
    if fileName.startswith('STOCK_AMOUNT'):
        amountdfItems.append(pd.read_hdf('h5data/'+fileName, key='df'))
        print(fileName)

topdf = pd.concat(topdfItems).sort_index(ascending=True)
amountdf = pd.concat(amountdfItems).sort_index(ascending=True)
print(topdf.index)

# snamedf = pd.read_hdf('h5data/SHARE_NAME.h5', key='df')
# scodedf = pd.read_hdf('h5data/SHARE_CODE.h5', key='df')
topdf = topdf[~topdf.index.duplicated(keep='first')]
#marcapdf = sl.loadMarcap()
marcapdf = pd.read_hdf('h5data/CODE_STOCKS_2006-01-01_2019_05_30.h5', key='df')
factordf = sl.loadFactor()
# qfactordf = sl.loadQuaterFactor()
intersect = list(set(topdf.columns) & set(topcap['Name'].values)) #+ ['KOSEF 국고채10년레버리지']
# intersect = list(map(lambda x : sCode[x],intersect))
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
endDate = pd.to_datetime('2019-11-04', format='%Y-%m-%d')
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
# factors = ['per', 'pcr', 'pbr', 'roe', 'psr', 'roic', 'eps', 'ebit', 'ev_ebit', 'ev_sales', 'ev_ebitda', '당기순이익률', '영업이익률', '매출총이익률', '배당수익률']
factors = ['per', 'pcr', 'pbr', 'roe', 'psr', 'roic', 'eps', 'ebit', 'ev_ebit', 'ev_sales', 'ev_ebitda', '당기순이익률', '영업이익률', '매출총이익률', '배당수익률']
weights = {'per': -0.38984704, 'pcr':-0.28457257, 'pbr':0.012896824, 'roe':-0.3275174, 'psr':-0.4753654, 'roic':0.038103256, 'eps':0.011919614, 'ebit':0.022765404, 'ev_ebit':0.052939765, 'ev_sales':0.017859187, 'ev_ebitda':-0.3338476, '당기순이익률':0.054088812, '영업이익률':-0.28509256, '매출총이익률':0.027673429, '배당수익률':-0.024145894}

investigation = []
rebalanceRate = 0
momentumList = []
konexCode = ['076340','223220','186230','112190','214610','224880','086220','284610','272420','178600','114920','199150','183410','176750','220110','086080','200350','271850','302920','270020','163430','216400','126340','232680','285770','215050','110660','135270','162120','210610','302550','185190','217320','233250','103660','189330','189540','267060','279600','149010','281310','208890','158300','086460','266170','203400','064850','260870','236030','271400','179720','216280','140660','267810','270660','121060','084440','221800','210120','101360','270210','299670','219750','140290','217910','189350','136660','116100','262760','199800','217880','202960','276240','225860','065370','107640','222670','121060','224760','225220','221670','299480','220250','207230','224810','228180','229000','229500','092590','217950','211050','230400','180060','225850','044990','232680','226610','258050','234070','277880','266870','278990','135160','215570','233990','176560','232530','206950','067370','236340','237720','222160','284420','238500','240340','241510','058970','242420','066830','228760','242850','112190','244880','245030','229480','208850','245450','246250','247300','224020','212310','191600','250300','239890','251960','252370','251280','243870','253840','199290','148780','167380','258540','232830','258250','227420','260970','205290','242350','120780']
defblackList = list(map(lambda x : sCode[x] if x in sCode.keys() else '', konexCode))
blackList = defblackList
maxValues = {}

highShare = []

while endDate >= current:
    if nextYearDay <= current:
        nextYearDay = current + pd.Timedelta(1, unit='Y')
        if len(momentumList) > 0:
            printG('momentumSumTotal',sum(momentumList), sum(momentumList)/len(momentumList))
        momentumList = []
        blackList = defblackList

    if nextInvestDay <= current or nextInvestDay.day == current.day:
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
            printG(pair['code'],sName[pair['code']], lastMoney/(pair['price']*ratio))
            if lastMoney/(pair['price']*ratio) > 1.3:
                highShare.append(pair['code'])

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
            buyDateIdx = moneySum.index.get_loc(buyDate, method='nearest')
            currentIdx = moneySum.index.get_loc(current, method='nearest')
            bDate = moneySum.index[buyDateIdx]
            cDate = moneySum.index[currentIdx]

            maxValue = moneySum.iloc[buyDateIdx:currentIdx+1].max()
            minValue = moneySum.iloc[buyDateIdx:currentIdx+1].min()
            meanValue = moneySum.iloc[buyDateIdx:currentIdx+1].mean()
            buyValue = moneySum.iloc[buyDateIdx]

            printG('monthPercentage: ', bDate,'~',cDate, ' # ', (moneySum.iloc[currentIdx] - moneySum.iloc[buyDateIdx])/moneySum.iloc[buyDateIdx] * 100 )
            printG('monthTotal: ', bDate,' # ', moneySum.iloc[buyDateIdx],"  ",cDate, ' # ', moneySum.iloc[currentIdx] )
            printG('lossPoint:', 1 - (maxValue - minValue)/meanValue)
            printG('maxPoint:', 1 + ((maxValue - buyValue)/buyValue))
            printG('minPoint:', 1 + ((minValue - buyValue)/buyValue))
        
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
        nextInvestDay = current + pd.Timedelta(32, unit='d')
        nextInvestDay = nextInvestDay.replace(day=1)
        target = list(topdf.columns)
        if len(blackList) > 0:
            target = list(filter(lambda x : x not in blackList, target ))
        target = ss.filterAltmanZScore(current, topdf[target], factordf, topcap, sName, sCode )
        printG('altman', len(target))
        inter = list(set(topdf.columns) & set(target))
        # if True:
            # target = ss.getFactorListsStd(current, topdf[inter], factordf, factors, 30, weights, sName, sCode)
        # else:
        target = ss.getFactorList(current, topdf[inter], factordf, 'roe', sName, sCode, False, 3000, minVal=0.00000001)
        # target = ss.getFactorList(current, topdf[target], factordf, 'eps증가율', sName, sCode, False, 3000, minVal=0)
        target = ss.getFactorList(current, topdf[target], factordf, '영업이익률', sName, sCode, False, 3000, minVal=0.00000001)
        target = ss.getFactorList(current, topdf[target], factordf, '당기순이익률', sName, sCode, False, 3000, minVal=3)
        target1 = ss.getFactorListComp(current, topdf[target], factordf, '매출총이익률', sName, sCode, False, 1000)
        target2 = ss.getVarienceList(current, topdf[target], 1000, True)
        target = list(set(target1)&set(target2))
        

        # if current.month == 12:
        #     target = ss.getCurValuePerStockNumFactor(current, topdf[target], factordf, '당기순이익', marcapdf, sCode, sName, 1000, True, int(len(target)/2), minVal=0.00000001)
        #     target = ss.getCurValuePerStockNumFactor(current, topdf[target], factordf, '영업활동으로인한현금흐름', marcapdf, sCode, sName, 1000, True, 50, minVal=0.00000001)
        #     target = ss.getFactorList(current, topdf[target], factordf, '배당수익률',sName, sCode, False, 30, minVal=3)

        # else:
        # target = ss.getCurValuePerStockNumFactor(current, topdf[target], factordf, '당기순이익', marcapdf, sCode, sName, 1000, True, int(len(target)/2), minVal=0.00000001)
        target = ss.getCurValuePerStockNumFactor(current, topdf[target], factordf, '당기순이익', marcapdf, sCode, sName, 1000, True, int(len(target)/2), minVal=0.00000001)
        target = ss.getCurValuePerStockNumFactor(current, topdf[target], factordf, '영업활동으로인한현금흐름', marcapdf, sCode, sName, 1000, True, 50, minVal=0.00000001)
        # target = ss.getCurValuePerStockNumFactor(current, topdf[target], factordf, '매출액', marcapdf, sCode, sName, 1000, True, 30, minVal=0.00000001)
        beforebeforeTarget = target

        # target = ss.getFactorList(current, topdf[target], factordf, '당기순이익률', sName, sCode, True, 30, minVal=3)
        target = ss.getFactorPerStockNum(current, topdf[target], factordf, '영업활동으로인한현금흐름', marcapdf, sCode, sName, True, 30, minVal=0.00000001)
        target = ss.getFactorList(current, topdf[target], factordf, '영업활동으로인한현금흐름',sName, sCode, False, 30, minVal=0.00000001)
        # target = ss.getFactorList(current, topdf[target], factordf, 'eps', False, 30, minVal=0)
        # target, momentumSum = ss.getMomentumList(current, topdf[target], mNum=24, mUnit='M', limit=30, minVal=0.00000001)
        # target = ss.getAmount(current, marcapdf, target, sName, sCode, limit=200000000)
        target = ss.getAmountLimitList(current, topdf[target], amountdf[target], limit=200000000)
        # target = ss.getVPCIUpListWeek(current, topdf[target], amountdf[target], 30)
        

        # beforeTarget = target
        # notMomentumTarget = target
        # if len(target) > 0:
        #     target, momentumSum = ss.getMomentumList(current, topdf[target], mNum=12, mUnit='M', limit=30, minVal=0.00000001)
        # only12MomentumTarget = target
        # if len(target) > 0:
        #     target, momentumSum = ss.getMomentumList(current, topdf[target], mNum=2, mUnit='M', limit=30, minVal=0.00000001)
        # printG('momentumSum', momentumSum)

        beforeTarget = target
        notMomentumTarget = target
        if len(target) > 0:
            target = ss.getMomentumListMonthCurrent(current, topdf[target], month=12, limit=30, minVal=0.00000001)
        only12MomentumTarget = target
        if len(target) > 0:
            target = ss.getMomentumListMonthCurrent(current, topdf[target], month=2, limit=30, minVal=0)
        # printG('momentumSum', momentumSum)
        
        printG('vpci')
        printG(ss.getVPCI(current, topdf[target], amountdf[target]))
        
        printG('vpciShort')
        printG(ss.getVPCIShort(current, topdf[target], amountdf[target]))
        
        printG('vpciLong')
        printG(ss.getVPCILong(current, topdf[target], amountdf[target]))

        target = list(set(target + highShare))
        highShare = []

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
            beforeDate = current - pd.Timedelta(1, unit='D')
            q = st.possibleQuantity(beforeDate, investMoney, stockName)
            if not q:
                continue
            so = StockOrder.create(stockName, q, investMoney)
            #일단사기
            buyMoney = st.getValue(beforeDate, stockName)
            if stockName not in maxValues:
                maxValues[stockName] = {'buy': buyMoney, 'max':buyMoney}
            else:
                if maxValues[stockName]['max'] < buyMoney:
                    maxValues[stockName]['max'] = buyMoney
            wallet.buy(so.code, so.quantity, buyMoney)
            printG('BUY', so.code,str(so.quantity)+'주')
            restMoney -= buyMoney * so.quantity
        
        beforeMonthTarget = target

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
        maxalreadyCut = []
        cutList = {}
        for stock in wallet.getAllStock():
            loss = st.calculateLosscutRate(stock['code'], current)
            printG(stock['code'], loss)
    #최대값 비율 손절 내일 아침에 팔기로 할 때
    for name in maxValues:
        if name in target:
            curVal = st.getValue(current, name)
            if maxValues[name]['max'] > curVal:
                curgap = curVal - maxValues[name]['buy']
                maxgap = maxValues[name]['max'] - maxValues[name]['buy']
                gapPercent = curgap / maxgap * 100
                topPercent = maxgap / maxValues[name]['buy'] * 100
                if gapPercent <= 50 and topPercent >= 30 and name not in maxalreadyCut:
                    maxalreadyCut.append(name)
                    printG('최대값 비율 손절: ', name, str(gapPercent) + '%', maxValues[name]['max'], maxValues[name]['buy'])
                    lossStock = wallet.getStock(name)
                    stockQuantity = lossStock['quantity']
                    sellMoney = st.getValue(current, name)
                    isSold = wallet.sell(name, stockQuantity, sellMoney)
                    if isSold:
                        restMoney += sellMoney * stockQuantity

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
    cutSum1, curValue1, beforeValue1 = st.getLosscutScalarSum(stockCodes, current, current + pd.Timedelta(-1, 'D'))
    cutSum2, curValue2, beforeValue2 = st.getLosscutScalarSum(stockCodes, current + pd.Timedelta(-1, 'D'), current + pd.Timedelta(-2, 'D'))
    cutSum3, curValue3, beforeValue3 = st.getLosscutScalarSum(stockCodes, current + pd.Timedelta(-2, 'D'), current + pd.Timedelta(-3, 'D'))
    cutSum4, curValue4, beforeValue4 = st.getLosscutScalarSum(stockCodes, current + pd.Timedelta(-3, 'D'), current + pd.Timedelta(-4, 'D'))
    cutSum5, curValue5, beforeValue5 = st.getLosscutScalarSum(stockCodes, current + pd.Timedelta(-4, 'D'), current + pd.Timedelta(-5, 'D'))
    lossnum = 0
    lossnum2 = 0
    # if cutSum1 < 0.99:
    #     lossnum +=1
    #     if cutSum2 < 0.99 or cutSum2 == 1:
    #         if cutSum2 != 1:
    #             lossnum +=1
    #         if cutSum3 < 0.99 or cutSum3 == 1:
    #             if cutSum3 != 1:
    #                 lossnum +=1
    #             if cutSum4 < 0.99 or cutSum4 == 1:
    #                 if cutSum4 != 1:
    #                     lossnum +=1
    #                 if cutSum5 < 0.99 or cutSum5 == 1:
    #                     if cutSum5 != 1:
    #                         lossnum +=1
    if cutSum1 < 0.98:
        lossnum2 +=1
        if cutSum2 < 0.98 or cutSum2 == 1:
            if cutSum2 != 1:
                lossnum2 +=1
            if cutSum3 < 0.98 or cutSum3 == 1:
                if cutSum3 != 1:
                    lossnum2 +=1
                if cutSum4 < 0.98 or cutSum4 == 1:
                    if cutSum4 != 1:
                        lossnum2 +=1
                    if cutSum5 < 0.98 or cutSum5 == 1:
                        if cutSum5 != 1:
                            lossnum2 +=1
    # printG('lossNum', lossnum, cutSum1, cutSum2, cutSum3, cutSum4, cutSum5)
    # printG('curValue', curValue1, curValue2, curValue3, curValue4, curValue5)
    # printG('beforeValue', beforeValue1, beforeValue2, beforeValue3, beforeValue4, beforeValue5)
    # if current > priceLimitDate:
    #     limitPercent = -0.1
    # else:
    allLimit = 0
    seqLimit = 0
    size = 0
    for stock in wallet.getAllStock():
        allLimit += (st.calculateLosscutRateRatio(stock['code'], current, 0.3))
        seqLimit += (st.calculateLosscutRateRatio(stock['code'], current, 0.5))
        size+=1
    size = 1 if size == 0 else size
    limitPercent = seqLimit - size
    # printG('lossCutAll',allLimit/size, 'lossCutSeq',limitPercent)
    cutSum, curValue, beforeValue = st.getLosscutScalarSum(stockCodes, current, buyDate)
    if cutSum <= allLimit/size or lossnum2 >= 2 or (cutSum1 + cutSum2 + cutSum3 + cutSum4 + cutSum5 - 5 <= limitPercent and len(target)>=4):
        li = []
        minusLen = 0
        for stock in wallet.getAllStock():
            val = st.getValue(current, stock['code'])
            li.append({'val':val / stock['money'], 'stock':stock})
            cut = st.getLosscutScalar(stock['code'], current, buyDate)
            if cut < 1:
                minusLen += 1
        li.sort(key=lambda data : data['val'])
        length = len(li)
        if len(li) >= 4:
            length = minusLen#int(len(li)/2)
        else:
            length = minusLen
        for d in li[0:length]:
            lossTarget = d['stock']['code']
            if lossTarget not in cutList.keys():
                alreadyCut.append(lossTarget)
                lossStock = wallet.getStock(lossTarget)
                stockQuantity = lossStock['quantity']
                sellMoney = st.getValue(current, lossTarget)
                isSold = wallet.sell(lossStock['code'], stockQuantity, sellMoney)
                if isSold:
                    cutList[lossStock['code']] = {'value':st.getValue(current, lossStock['code']), 'money':sellMoney * stockQuantity}
                    printG('손절갯수:', len(cutList.keys()), cutList.keys())
                    restMoney += sellMoney * stockQuantity
    #     #손절
    for stock in wallet.getAllStock():   
        cut1 = st.getLosscutScalar(stock['code'], current, current + pd.Timedelta(-1, 'D'))
        cut2 = st.getLosscutScalar(stock['code'], current + pd.Timedelta(-1, 'D'), current + pd.Timedelta(-2, 'D'))
        cut3 = st.getLosscutScalar(stock['code'], current + pd.Timedelta(-2, 'D'), current + pd.Timedelta(-3, 'D'))
        cut4 = st.getLosscutScalar(stock['code'], current + pd.Timedelta(-3, 'D'), current + pd.Timedelta(-4, 'D'))
        cut5 = st.getLosscutScalar(stock['code'], current + pd.Timedelta(-4, 'D'), current + pd.Timedelta(-5, 'D'))
        if current > priceLimitDate:
            limitPercent = -0.2
        else:
            limitPercent = -0.1
        isUnder10Per = cut1 + cut2 + cut3 + cut4 + cut5 - 5 <= limitPercent
        if isUnder10Per and stock['code'] not in cutList.keys():
            lossStock = wallet.getStock(stock['code'])
            stockQuantity = lossStock['quantity']
            sellMoney = st.getValue(current, stock['code'])
            isSold = wallet.sell(lossStock['code'], stockQuantity, sellMoney)
            if isSold:
                cutList[stock['code']] = {'value':st.getValue(current, stock['code']), 'money':sellMoney * stockQuantity}
                printG('종목마다손절갯수:', len(cutList.keys()), cutList.keys())
                restMoney += sellMoney * stockQuantity
    #vpci down 손절
    # lossstocks = []
    # for stock in wallet.getAllStock():
    #     lossstocks.append(stock['code'])
    # lists = ss.getVPCIDownListWeek(current, topdf[lossstocks], amountdf[lossstocks])
    # for item in lists:
    #     if item not in cutList.keys():
    #         lossStock = wallet.getStock(item)
    #         stockQuantity = lossStock['quantity']
    #         sellMoney = st.getValue(current, lossStock['code'])
    #         isSold = wallet.sell(lossStock['code'], stockQuantity, sellMoney)
    #         if isSold:
    #             cutList[lossStock['code']] = {'value':st.getValue(current, lossStock['code']), 'money':sellMoney * stockQuantity}
    #             printG('vpci 손절:', len(cutList.keys()), cutList.keys())
    #             restMoney += sellMoney * stockQuantity
    #다시 들어가기
    # delList = []
    # for code in cutList:
    #     curValue = st.getValue(current, code)
    #     if cutList[code]['value'] < curValue:
    #         printG('다시 사기:', len(cutList.keys()))
    #         buyAllMoney = 0
    #         if restMoney > cutList[code]['money']:
    #             buyAllMoney = cutList[code]['money']
    #         else:
    #             buyAllMoney = restMoney
    #         q = st.possibleQuantity(current, buyAllMoney, code)
    #         if not q:
    #             continue
    #         so = StockOrder.create(code, q, investMoney)
    #         #일단사기
    #         buyMoney = st.getValue(current, code)
    #         wallet.buy(so.code, so.quantity, buyMoney)
    #         restMoney -= buyMoney * so.quantity
    #         delList.append(code)
    # for item in delList:
    #     cutList.pop(item)

                # if stock['code'] not in losscutTarget:
                #     losscutTarget.append(stock['code'])
                #     if len(losscutTarget) >= len(target):
                #         printG('손절갯수:', len(losscutTarget))
                #         for lossTarget in losscutTarget:
                #             if lossTarget not in alreadyCut:
                #                 alreadyCut.append(lossTarget)
                #                 lossStock = wallet.getStock(lossTarget)
                #                 stockQuantity = lossStock['quantity']
                #                 sellMoney = st.getValue(current, lossTarget)
                #                 isSold = wallet.sell(lossStock['code'], stockQuantity, sellMoney)
                #                 if isSold:
                #                     restMoney += sellMoney * stockQuantity

    #손절 및 다시 들어가기
    #손절
    # for stock in wallet.getAllStock():   
    #     isLosscutScalar = st.losscutScalar(stock['code'], current, buyDate, 0.95)
    #     if isLosscutScalar and stock['code'] not in cutList.keys():
    #         lossStock = wallet.getStock(stock['code'])
    #         stockQuantity = lossStock['quantity']
    #         sellMoney = st.getValue(current, stock['code'])
    #         isSold = wallet.sell(lossStock['code'], stockQuantity, sellMoney)
    #         if isSold:
    #             cutList[stock['code']] = {'value':st.getValue(current, stock['code']), 'money':sellMoney * stockQuantity}
    #             printG('손절갯수:', len(cutList.keys()))
    #             restMoney += sellMoney * stockQuantity
    # delList = []
    # # #다시 들어가기
    # for code in cutList:
    #     curValue = st.getValue(current, code)
    #     if cutList[code]['value'] < curValue:
    #         printG('다시 사기:', len(cutList.keys()))
    #         buyAllMoney = 0
    #         if restMoney > cutList[code]['money']:
    #             buyAllMoney = cutList[code]['money']
    #         else:
    #             buyAllMoney = restMoney
    #         q = st.possibleQuantity(current, buyAllMoney, code)
    #         if not q:
    #             continue
    #         so = StockOrder.create(code, q, investMoney)
    #         #일단사기
    #         buyMoney = st.getValue(current, code)
    #         wallet.buy(so.code, so.quantity, buyMoney)
    #         restMoney -= buyMoney * so.quantity
    #         delList.append(code)
    # for item in delList:
    #     cutList.pop(item)

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
    beforeDay = current + pd.Timedelta(-1, unit='D')
    money = stockMoney + restMoney + rebalaceMoney
    moneySum.loc[current] = money
    if moneySum.index.contains(beforeDay):
        beforeMoney = moneySum.loc[beforeDay]
        upDownMoney = str((money - beforeMoney))
        upDownPercent = str(((money - beforeMoney) / beforeMoney * 100)) + '%'
        upDownSign = '▲' if beforeMoney < money else '▼'
        upDownSign = upDownSign if beforeMoney != money else '∙'
        upDown = upDownSign + ' ' + upDownMoney + ' ' + upDownPercent
    else:
        upDown = ''
    printG(current, money, stockMoney, restMoney, rebalaceMoney, upDown)
    current = nextDay
# In[3]:
current = endDate
target = list(topdf.columns)
if len(blackList) > 0:
    target = list(filter(lambda x : x not in blackList, target ))
target = ss.filterAltmanZScore(current, topdf[target], factordf, topcap, sName, sCode )
printG('altman', len(target))
inter = list(set(topdf.columns) & set(target))
target = ss.getFactorList(current, topdf[inter], factordf, 'roe', sName, sCode, False, 3000, minVal=0.00000001)
# target = ss.getFactorList(current, topdf[target], factordf, 'eps증가율', sName, sCode, False, 3000, minVal=0)
target = ss.getFactorList(current, topdf[target], factordf, '영업이익률', sName, sCode, False, 3000, minVal=0.00000001)
target = ss.getFactorList(current, topdf[target], factordf, '당기순이익률', sName, sCode, False, 3000, minVal=3)
target1 = ss.getFactorListComp(current, topdf[target], factordf, '매출총이익률', sName, sCode, False, 1000)
target2 = ss.getVarienceList(current, topdf[target], 1000, True)
target = list(set(target1)&set(target2))


# if current.month == 12:
#     target = ss.getCurValuePerStockNumFactor(current, topdf[target], factordf, '당기순이익', marcapdf, sCode, sName, 1000, True, int(len(target)/2), minVal=0.00000001)
#     target = ss.getCurValuePerStockNumFactor(current, topdf[target], factordf, '영업활동으로인한현금흐름', marcapdf, sCode, sName, 1000, True, 50, minVal=0.00000001)
#     target = ss.getFactorList(current, topdf[target], factordf, '배당수익률',sName, sCode, False, 30, minVal=3)

# else:
# target = ss.getCurValuePerStockNumFactor(current, topdf[target], factordf, '당기순이익', marcapdf, sCode, sName, 1000, True, int(len(target)/2), minVal=0.00000001)
target = ss.getCurValuePerStockNumFactor(current, topdf[target], factordf, '당기순이익', marcapdf, sCode, sName, 1000, True, int(len(target)/2), minVal=0.00000001)
target = ss.getCurValuePerStockNumFactor(current, topdf[target], factordf, '영업활동으로인한현금흐름', marcapdf, sCode, sName, 1000, True, 50, minVal=0.00000001)
# target = ss.getCurValuePerStockNumFactor(current, topdf[target], factordf, '매출액', marcapdf, sCode, sName, 1000, True, 30, minVal=0.00000001)
beforebeforeTarget = target

# target = ss.getFactorList(current, topdf[target], factordf, '당기순이익률', sName, sCode, True, 30, minVal=3)
target = ss.getFactorPerStockNum(current, topdf[target], factordf, '영업활동으로인한현금흐름', marcapdf, sCode, sName, True, 30, minVal=0.00000001)
target = ss.getFactorList(current, topdf[target], factordf, '영업활동으로인한현금흐름',sName, sCode, False, 30, minVal=0.00000001)
# target = ss.getFactorList(current, topdf[target], factordf, 'eps', False, 30, minVal=0)
# target, momentumSum = ss.getMomentumList(current, topdf[target], mNum=24, mUnit='M', limit=30, minVal=0.00000001)
# target = ss.getAmount(current, marcapdf, target, sName, sCode, limit=200000000)
target = ss.getAmountLimitList(current, topdf[target], amountdf[target], limit=200000000)
# target = ss.getVPCIUpListWeek(current, topdf[target], amountdf[target], 30)


# beforeTarget = target
# notMomentumTarget = target
# if len(target) > 0:
#     target, momentumSum = ss.getMomentumList(current, topdf[target], mNum=12, mUnit='M', limit=30, minVal=0.00000001)
# only12MomentumTarget = target
# if len(target) > 0:
#     target, momentumSum = ss.getMomentumList(current, topdf[target], mNum=2, mUnit='M', limit=30, minVal=0.00000001)
# printG('momentumSum', momentumSum)

#getMomentumListMonthCurrent
printG('#####getMomentumListMonthCurrent')
if len(notMomentumTarget) > 0:
    target = ss.getMomentumListMonthCurrent(current, topdf[notMomentumTarget], month=12, limit=30, minVal=0.00000001)
only12MomentumTarget = target
if len(target) > 0:
    target = ss.getMomentumListMonthCurrent(current, topdf[target], month=2, limit=30, minVal=0.00000001)
only2MomentumTarget = []
if len(target) > 0:
    only2MomentumTarget = ss.getMomentumListMonthCurrent(current, topdf[notMomentumTarget], month=2, limit=30, minVal=0.00000001)

printG('notMomentumTarget', notMomentumTarget)
printG('only12MomentumTarget', only12MomentumTarget)
printG('only2MomentumTarget', only2MomentumTarget)
printG('lastTarget2', target)

for name in maxValues:
    if name in target:
        curVal = st.getValue(current, name)
        if maxValues[name]['max'] > curVal:
            curgap = curVal - maxValues[name]['buy']
            maxgap = maxValues[name]['max'] - maxValues[name]['buy']
            gapPercent = curgap / maxgap * 100
            topPercent = maxgap / maxValues[name]['buy'] * 100
            if gapPercent <= 50 and topPercent >= 30 and name not in alreadyCut:
                printG('최대값 비율 손절: ', name, str(gapPercent) + '%', maxValues[name]['max'], maxValues[name]['buy'])
    

# In[4]: look
# moneySum
# In[3]: 통계
moneySum.index = moneySum.index.map(lambda dt: pd.to_datetime(dt.date()))
portfolio = moneySum / 10000000

# 투자기간 = len(moneySum.index)/12
# print(portfolio)
투자기간 = (portfolio.index[-1] - portfolio.index[0]).days/365
print('투자기간',투자기간)
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

