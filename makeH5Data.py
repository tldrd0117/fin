# In[0]:
import pandas as pd
from simulator.MongoStockCollection import MongoStockCollection
from simulator.StockLoader import StockLoader
import pymongo

msc = MongoStockCollection.create()
sl = StockLoader.create()
stockDb = msc.get()
topdf = pd.DataFrame()
amountdf = pd.DataFrame()

topcap, allCodes, allNames = sl.loadTopcapDf()
share = ''
pipeline = [ { '$sort': {'날짜':1} } ]
data = stockDb.aggregate(pipeline, allowDiskUse=True)
for obj in data:
    if share != obj['날짜']:
        share = obj['날짜']
        print(share)

    topdf.at[pd.to_datetime(obj['날짜']),allCodes[obj['종목코드']]] = obj['종가']
    amountdf.at[pd.to_datetime(obj['날짜']),allCodes[obj['종목코드']]] = obj['거래량']
topdf.to_hdf('h5data/STOCK_CLOSE_2006-01-01_2019-08-23.h5', key='df')
amountdf.to_hdf('h5data/STOCK_AMOUNT_2006-01-01_2019-08-23.h5', key='df')

# In[1]: CodeName
# import pandas as pd
# from simulator.MongoStockCollection import MongoStockCollection
# import pymongo
# msc = MongoStockCollection.create()
# stockDb = msc.get()
# sName = pd.Series()
# sCode = pd.Series()
# data = stockDb.aggregate([{"$group":{"_id":{'종목명': '$종목명', '종목코드':'$종목코드'}}}])
# for obj in data:
#     sName.at[obj['_id']['종목명']]=obj['_id']['종목코드']
#     sCode.at[obj['_id']['종목코드']]=obj['_id']['종목명']
# sName.to_hdf('h5data/SHARE_NAME.h5', key='df')
# sCode.to_hdf('h5data/SHARE_CODE.h5', key='df')


# # In[1]:
# import pandas as pd
# from simulator.StockStrategy import StockStrategy

# pd.options.display.float_format = '{:.2f}'.format
# topdf = pd.read_hdf('h5data/STOCK_CLOSE_2006-01-01_2019-08-23.h5', key='df')
# amountdf = pd.read_hdf('h5data/STOCK_AMOUNT_2006-01-01_2019-08-23.h5', key='df')
# # (topdf*amountdf).mean()
# ss = StockStrategy.create()
# current = pd.to_datetime('2019-08-01', format='%Y-%m-%d')
# target = ['대한해운', '일지테크', '캠시스', '엠씨넥스', '계룡건설', '삼호', '제일테크노스', '세원물산', '세방전지', '신풍제지']
# print(amountdf[target] * topdf[target])
# target = ss.getAmountLimitList(current, topdf[target], amountdf[target], limit=200000000)
# target

#%%
