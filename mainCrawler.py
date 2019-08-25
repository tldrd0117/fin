# In[0]:
from simulator.StockLoader import StockLoader
from simulator.MongoStockCollection import MongoStockCollection

msc = MongoStockCollection.create()
stockDb = msc.get()
sl = StockLoader.create()

alreadyIn = stockDb.distinct('종목코드')
print(alreadyIn)

topcap = sl.load(sl.makeName('TOPCAP', '2007-01-01', '2019-12-31'))
sCode = {row['Code'] : row['Name'] for index, row  in topcap.iterrows()}
codes = list(set(topcap['Code']))
targets = [ {'Code':code, 'Name':sCode[code]} for code in codes if code not in alreadyIn ]

name = sl.makeNameJson('STOCK_DATA', '2006-01-01', '2019-8-24')
data = sl.loadStockMongo(name, targets, '2006-01-01', '2019-8-24',stockDb)
# print(data)

#%%
