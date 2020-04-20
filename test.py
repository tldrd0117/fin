
# In[0]:
import pandas as pd
from simulator.StockLoader import StockLoader

sl = StockLoader.create()
qdf = sl.loadQuaterFactor()
qdf['이익잉여금'][float('2018.06')]

# In[1]: 압축
import gzip
with open("h5data/CODE_STOCKS_2006-01-01_2019_05_30.h5", "rb") as file_in:
    # Write output.
    with gzip.open("h5data/CODE_STOCKS_2006-01-01_2019_05_30.h5.gz", "wb") as file_out:        
        file_out.writelines(file_in)
# In[2]: 압출 풀기
import pandas as pd
import io
import gzip
with gzip.open("h5data/CODE_MARCAP_2006-01-01_2019_05_30.h5.gz", "rb") as f:
    # Read in string.
    content = f.read()
    # Print length.
    print(len(content))
with open('h5data/CODE_MARCAP_2006-01-01_2019_05_30.h5', 'wb') as f:
    f.write(content)
pd.read_hdf('h5data/CODE_MARCAP_2006-01-01_2019_05_30.h5', key='df')

# In[3]:
import pandas as pd
data = pd.read_hdf('h5data/CODE_MARCAP_2006-01-01_2019_05_30.h5', key='df')

current = pd.to_datetime('2019-04-24', format='%Y-%m-%d')
endDate = current + pd.Timedelta(-91,unit='D')
data.loc[endDate:current].resample('30D').mean()

# In[4]:
import pandas as pd
from simulator.StockLoader import StockLoader
sl = StockLoader.create()
assets = sl.loadFactor()
factors = ['per', 'pcr', 'pbr', 'roe', '당기순이익', '영업활동으로인한현금흐름', '투자활동으로인한현금흐름', '재무활동으로인한현금흐름', 'psr', 'roic', 'eps', 'ebit', 'ev_ebit', 'ev_sales', 'ev_ebitda', '당기순이익률', '영업이익률', '매출총이익률', '배당수익률', '매출액', '자산', '유동자산', '부채', '유동부채', '이익잉여금']
targets = ['당기순이익','영업활동으로인한현금흐름','투자활동으로인한현금흐름', '재무활동으로인한현금흐름','당기순이익률', '영업이익률', '매출총이익률', '배당수익률', '매출액', '자산', '유동자산', '부채', '유동부채', '이익잉여금']
result = "["
for v in targets:
    t = assets[v]
    for c in t.index.values:
        row = t.loc[c]
        for year in range(2007,2019):
            if(len(result)!=1):
                result+=" ,\n"
            result += "{'종목코드':'%s', '년도':'%s', '종목명':'%s', '데이터명':'%s', '데이터':'%s'}" % (c,year,row.loc["종목명"], v, row.loc[float(year)])
result += "]"
# result
with open("finData.json", "w") as text_file:
    text_file.write(result)


# %%
