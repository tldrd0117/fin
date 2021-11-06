
from simulator.StockLoader import StockLoader

sl = StockLoader.create()

a = sl.loadFactorMerge()
print(a)