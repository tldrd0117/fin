# In[0]:
import pandas as pd
from simulator.MongoStockCollection import MongoStockCollection
import pymongo
msc = MongoStockCollection.create()
stockDb = msc.get()
topdf = pd.DataFrame()
amountdf = pd.DataFrame()
share = ''
pipeline = [ { '$sort': {'날짜':1} } ]
data = stockDb.aggregate(pipeline, allowDiskUse=True)
for obj in data:
    if share != obj['날짜']:
        share = obj['날짜']
        print(share)
    topdf.at[pd.to_datetime(obj['날짜']),obj['종목명']] = obj['종가']
    amountdf.at[pd.to_datetime(obj['날짜']),obj['종목명']] = obj['거래량']
topdf.to_hdf('h5data/STOCK_CLOSE_2006-01-01_2019-08-23.h5', key='df')
amountdf.to_hdf('h5data/STOCK_AMOUNT_2006-01-01_2019-08-23.h5', key='df')

# In[1]:
import pandas as pd
from simulator.StockStrategy import StockStrategy

pd.options.display.float_format = '{:.2f}'.format
topdf = pd.read_hdf('h5data/STOCK_CLOSE_2006-01-01_2019-08-23.h5', key='df')
amountdf = pd.read_hdf('h5data/STOCK_AMOUNT_2006-01-01_2019-08-23.h5', key='df')
# (topdf*amountdf).mean()
ss = StockStrategy.create()
current = pd.to_datetime('2019-08-01', format='%Y-%m-%d')
target = ['대한해운', '일지테크', '캠시스', '엠씨넥스', '계룡건설', '삼호', '제일테크노스', '세원물산', '세방전지', '신풍제지']
print(amountdf[target] * topdf[target])
target = ss.getAmountLimitList(current, topdf[target], amountdf[target], limit=200000000)
target

#%%
