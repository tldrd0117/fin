# In[0]: 크롤링 후 db에 넣는다
import pandas as pd
# from simulator.MongoStockCollection import MongoStockCollection
from simulator.StockLoader import StockLoader
# import pymongo

#
# msc = MongoStockCollection.create()
# stockDb = msc.get()
sl = StockLoader.create()

alreadyIn = []#stockDb.distinct('종목코드')
print(alreadyIn)
    
startDate = '2021-10-06'
endDate = '2021-10-07'

topcap, allCodes, allNames = sl.loadTopcapDf(maxMarketCap=float('inf'))

sCode = {row['Code'] : row['Name'] for index, row  in topcap.iterrows()}
codes = list(set(topcap['Code']))
targets = [ {'Code':code, 'Name':sCode[code]} for code in codes if code not in alreadyIn ]

name = sl.makeNameJson('STOCK_DATA', startDate, endDate)
data = sl.loadStockArr(name, targets, startDate, endDate)
# sl.loadStockMongo(name, targets, startDate, endDate,stockDb)

# # In[2]: DB에서 가져와서 h5를 만든다
# import pandas as pd
# from simulator.MongoStockCollection import MongoStockCollection
# from simulator.StockLoader import StockLoader
# import pymongo
# import datetime as dt

# # startDate = '2020-10-16'
# # endDate = '2020-10-18'
# sl = StockLoader.create()
# msc = MongoStockCollection.create()
# stockDb = msc.get()
topdf = pd.DataFrame()
amountdf = pd.DataFrame()
# topcap, allCodes, allNames = sl.loadTopcapDf(maxMarketCap=float('inf'))
share = ''
# startDateTime = dt.datetime.strptime(startDate, '%Y-%m-%d')
# endDateTime = dt.datetime.strptime(endDate, '%Y-%m-%d')
# pipeline = [{ '$match':
#                 {'날짜': {\
#                     '$gte': startDateTime\
#                     ,'$lte': endDateTime\
#                 }}\
#             },\
#             {'$sort': {'날짜':1}}]
# data = stockDb.aggregate(pipeline, allowDiskUse=True)
for obj in data:
    if share != obj['날짜']:
        share = obj['날짜']
        print(share)

    topdf.at[pd.to_datetime(obj['날짜']),allCodes[obj['종목코드']]] = obj['종가']
    amountdf.at[pd.to_datetime(obj['날짜']),allCodes[obj['종목코드']]] = obj['거래량']
topdf.to_hdf('h5data/STOCK_CLOSE_'+startDate+'_'+endDate+'.h5', key='df')
amountdf.to_hdf('h5data/STOCK_AMOUNT_'+startDate+'_'+endDate+'.h5', key='df')
print('종가: ','h5data/STOCK_CLOSE_'+startDate+'_'+endDate+'.h5')
print('거래량: ','h5data/STOCK_AMOUNT_'+startDate+'_'+endDate+'.h5')

#In[3]:
topdf


#%%
