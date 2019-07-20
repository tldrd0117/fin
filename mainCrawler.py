# In[0]:
# from simulator.StockLoader import StockLoader
# sl = StockLoader.create()
# stockdf = sl.loadStockDf()
import matplotlib.pyplot as plt
import pandas as pd
choosedDf = pd.Series([0,1,2,3,4,5], index=[0,1,2,3,4,5])
print(choosedDf)
# jisuDf = choosedDf / choosedDf.iloc[0]
plot = choosedDf.plot(figsize = (18,12), fontsize=12)
# fontProp = fm.FontProperties(fname=path, size=18)
# plt.legend(prop=fontProp)

plt.show()


#%%
