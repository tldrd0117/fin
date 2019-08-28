
# In[0]:
import pandas as pd
from simulator.StockLoader import StockLoader

sl = StockLoader.create()
qdf = sl.loadQuaterFactor()
qdf['이익잉여금'][float('2018.06')]

# In[1]: 압축
import gzip
with open("h5data/CODE_MARCAP_2006-01-01_2019_05_30.h5", "rb") as file_in:
    # Write output.
    with gzip.open("h5data/CODE_MARCAP_2006-01-01_2019_05_30.h5.gz", "wb") as file_out:        
        file_out.writelines(file_in)
# In[2]: 압출 풀기
import pandas as pd
import io
with gzip.open("h5data/CODE_MARCAP_2006-01-01_2019_05_30.h5.gz", "rb") as f:
    # Read in string.
    content = f.read()
    # Print length.
    print(len(content))
with open('h5data/CODE_MARCAP_2006-01-01_2019_05_30.h5', 'wb') as f:
    f.write(content)
pd.read_hdf('h5data/CODE_MARCAP_2006-01-01_2019_05_30.h5', key='df')



#%%
