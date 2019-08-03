# In[0]:
from datetime import datetime, timedelta
now = datetime.now()
before = now - timedelta(days=200)
now = '%04d-%02d-%02d' % (now.year, now.month, now.day)
before = '%04d-%02d-%02d' % (before.year, before.month, before.day)

# In[1]:
# 종목코드와 종목이름으로 분류해서 사용한다
import pandas as pd
for year in range(2007,2020):
    kospiCompanyDf = pd.read_excel('finData/시가총액_2019_01_02.xlsx',sheet_name='시가총액', skiprows=3, converters={'종목코드':str})
    kospiCompanyDf = kospiCompanyDf.iloc[1:]
codeName = {}
for index, row in kospiCompanyDf.iterrows():
    codeName[row['종목코드']] = row['종목명']
codeName

# In[2]: crawling 현재가...
from simulator.StockLoader import StockLoader
sl = StockLoader.create()
topdf, topcap = sl.loadStockDf()
factordf = sl.loadFactor()
topdf

# In[3]:
