
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
# logging.basicConfig(handlers=[logging.FileHandler('simulation3.log', 'w', 'utf-8')], level=logging.INFO, format='%(message)s')
pd.set_option('display.float_format', None)
np.set_printoptions(suppress=True)
def printG(*msg):
    joint = ' '.join(list(map(lambda x : str(x), msg)))
    print(joint)
#     logging.info(joint)

# In[1]:

#StockLoader 시간에따른 주식 가격을 가져온다

sl = StockLoader.create()

topdf1, topcap, sCode = sl.loadTopDf()
topdf2, topcap2, sCode2 = sl.loadTopLately('2019-05-01','2019-08-04')
topdf3, topcap3, sCode3 = sl.loadTopLately('2019-08-05','2019-08-05')
topdf = pd.concat([topdf1, topdf2, topdf3])
topdf = topdf[~topdf.index.duplicated(keep='first')]

# marcapdf = sl.loadMarcap()
factordf = sl.loadFactor()
ss = StockStrategy.create()
st = StockTransaction.create(topdf)

konexCode = ['076340','223220','186230','112190','214610','224880','086220','284610','272420','178600','114920','199150','183410','176750','220110','086080','200350','271850','302920','270020','163430','216400','126340','232680','285770','215050','110660','135270','162120','210610','302550','185190','217320','233250','103660','189330','189540','267060','279600','149010','281310','208890','158300','086460','266170','203400','064850','260870','236030','271400','179720','216280','140660','267810','270660','121060','084440','221800','210120','101360','270210','299670','219750','140290','217910','189350','136660','116100','262760','199800','217880','202960','276240','225860','065370','107640','222670','121060','224760','225220','221670','299480','220250','207230','224810','228180','229000','229500','092590','217950','211050','230400','180060','225850','044990','232680','226610','258050','234070','277880','266870','278990','135160','215570','233990','176560','232530','206950','067370','236340','237720','222160','284420','238500','240340','241510','058970','242420','066830','228760','242850','112190','244880','245030','229480','208850','245450','246250','247300','224020','212310','191600','250300','239890','251960','252370','251280','243870','253840','199290','148780','167380','258540','232830','258250','227420','260970','205290','242350','120780']
blackList = list(map(lambda x : sCode[x] if x in sCode.keys() else '', konexCode))


# In[2]:
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
for key in factordf.keys():
    factordf[key].columns = list(map(lambda col : float(col) if isNumber(col) or col.isnumeric() else col, factordf[key].columns))
for key in factordf.keys():
    factordf[key] = factordf[key].set_index(['종목명'])
for key in factordf.keys():
    factordf[key] = toNumeric(factordf[key])
# In[3]:


current = pd.to_datetime('2019-08-04', format='%Y-%m-%d')


target = list(topdf.columns)
if len(blackList) > 0:
    target = list(filter(lambda x : x not in blackList, target ))
target = ss.filterAltmanZScore(current, topdf[target], factordf, topcap )
printG('altman', len(target))
# target = ss.getFactorLists(current, topdf[target], factordf, factors, 20, weights)
# target = ss.getRiseMeanList(current, topdf[target], 500, 0)
# target = ss.getFactorList(current, topdf[target], factordf, 'eps', False, 3000, minVal=100)
#roe 0 => 저 per반 => 영업수익률 1000 => pcr 50 => 당기순수익률 30  

# if ss.isUnemployedYear(current.year):
    # minRoe = 20
    # maxRoe = 40
# else:
minRoe = 0
maxRoe = 10000
# target = ss.getAmount(current, marcapdf, sCode, limit=500000000)
target = ss.getFactorList(current, topdf[target], factordf, 'roe', False, 3000, minVal=minRoe, maxVal=maxRoe)
target = ss.getFactorList(current, topdf[target], factordf, 'per', True, int(len(target)/2), minVal=0)
target = ss.getFactorList(current, topdf[target], factordf, '영업이익률', True, 1000, minVal=0)
target = ss.getFactorList(current, topdf[target], factordf, 'pcr', True, 50, minVal=0)
target = ss.getFactorList(current, topdf[target], factordf, '당기순이익률', True, 30, minVal=0)
beforeTarget = target
target, momentumSum = ss.getMomentumList(current, topdf[target], mNum=2, mUnit='M', limit=30, minVal=0)
printG('momentumSum', momentumSum)
if ss.isUnemployedYear(current.year):
    printG('isUnemployedYear')
    target = beforeTarget
# In[4]:SHOW
printG(target)
for code in target:
    rate = st.calculateLosscutRate(code, current)
    printG(code, rate)
