
# In[0]:
import pandas as pd
from simulator.StockLoader import StockLoader

sl = StockLoader.create()
qdf = sl.loadQuaterFactor()

# In[1]:
qdf['이익잉여금'][float('2018.06')]


#%%
