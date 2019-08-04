
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
topdf, topcap, sCode = sl.loadTopLately('2019-01-01','2019-08-04')
# marcapdf = sl.loadMarcap()
factordf = sl.loadFactor()
ss = StockStrategy.create()

current = pd.to_datetime('2019-08-04', format='%Y-%m-%d')


target = list(topdf.columns)
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
printG(target)
        

