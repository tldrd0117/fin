# In[0]:
import pandas as pd
from simulator.MongoStockCollection import MongoStockCollection
msc = MongoStockCollection.create()
stockDb = msc.get()
topdf = pd.DataFrame()
share = ''
for obj in stockDb.find():
    if share != obj['종목명']:
        share = obj['종목명']
        print(share)
    topdf.at[pd.to_datetime(obj['날짜']),obj['종목명']] = obj['종가']
topdf

#%%
