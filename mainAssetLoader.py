import pandas as pd
import pymongo
from simulator.MongoAssetCollection import MongoAssetCollection
from simulator.StockLoader import StockLoader

mac = MongoAssetCollection.create()
assetdb = mac.get()
sl = StockLoader.create()
assets = sl.loadFactor(2018)
factors = ['per', 'pcr', 'pbr', 'roe', '당기순이익', '영업활동으로인한현금흐름', '투자활동으로인한현금흐름', '재무활동으로인한현금흐름', 'psr', 'roic', 'eps', 'ebit', 'ev_ebit', 'ev_sales', 'ev_ebitda', '당기순이익률', '영업이익률', '매출총이익률', '배당수익률', '매출액', '자산', '유동자산', '부채', '유동부채', '이익잉여금']
targets = ['당기순이익','영업활동으로인한현금흐름','투자활동으로인한현금흐름', '재무활동으로인한현금흐름','당기순이익률', '영업이익률', '매출총이익률', '배당수익률', '매출액', '자산', '유동자산', '부채', '유동부채', '이익잉여금', 'roe', 'ebit', 'eps']
#targets = ['자산', '유동자산', '부채', '유동부채', '이익잉여금', 'roe','ebit']
result = "["
for v in targets:
    t = assets[v]
    for c in t.index.values:
        row = t.loc[c]
        for year in range(2007, 2019):
            print("year: "+str(year)+" "+v)
            assetDict = { \
                '종목코드': c, \
                '년도': year, \
                '종목명': row.loc["종목명"], \
                '데이터명': v, \
                '데이터': row.loc[float(year)]\
            }
            assetdb.insert(assetDict)
print("complete")