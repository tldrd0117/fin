import os
import pandas as pd
import time

from crawler.naver.NaverTopMarketCapCrawler import NaverTopMarketCapCrawler
from crawler.naver.data.NaverDate import NaverDate
from crawler.naver.NaverStockCrawler import NaverStockCrawler
from crawler.naver.DartCrawler import DartCrawler
from crawler.naver.NavarSearchCodeCrawler import NavarSearchCodeCrawler
from crawler.naver.NaverCrawler import NaverCrawler
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

pd.set_option('display.float_format', None)
class StockLoader:
    @staticmethod
    def create():
        stockloader = StockLoader()
        return stockloader
    
    def makeName(self, name, beforeStr, endDateStr):
        return 'h5data/'+name + '_' + beforeStr + '_' + endDateStr + '.h5'

    def topk(self, num):
        crawler = NaverTopMarketCapCrawler.create()
        data = crawler.crawling([1,num])
        codes = [ {'code':item.code, 'name':item.name}  for item in data ]
        return codes

    def load(self, name):
        print(name, 'read...')
        return pd.read_hdf(name, key='df')

    def filterETF(self):
        # In[23]: ETF 분할
        self.KODEX = self.loadSearchCodeName('KODEX')
        self.TIGER = self.loadSearchCodeName('TIGER')
        self.KOSEF = self.loadSearchCodeName('KOSEF')
        #KODEX 종목 (삼성자산운용)

        blackWords = ['액티브', '삼성']
        bondWords = [' 국고채', ' 국채']
        foreignWords = ['미국', '대만', '중국', '심천', '선진국', '일본', 'China', '글로벌', '차이나', '라틴', '유로', '인도', '하이일드']
        inverseWords = ['인버스']
        productWords = ['원유', '골드선물', '금선물', '은선물', '달러선물', '농산물', '금속선물', '금은선물', '엔선물', '구리선물', '구리실물', '콩선물']
        self.KODEX = self.exceptCodeName(blackWords, self.KODEX)
        self.KODEX_bond = self.chooseCodeName(bondWords, self.KODEX)
        self.KODEX_foreign = self.chooseCodeName(foreignWords, self.KODEX)
        self.KODEX_domestic = self.exceptCodeName(bondWords + foreignWords + inverseWords + productWords, self.KODEX)
        self.KODEX_exceptProduct = self.exceptCodeName(productWords + foreignWords, self.KODEX)
        self.KODEX_inverse = self.chooseCodeName(inverseWords, self.KODEX_exceptProduct)
        self.KODEX_product = self.chooseCodeName(productWords, self.KODEX)


        #TIGER 종목 (미래에셋대우)
        self.TIGER = self.exceptCodeName(blackWords, self.TIGER)
        self.TIGER_bond = self.chooseCodeName(bondWords, self.TIGER)
        self.TIGER_foreign = self.chooseCodeName(foreignWords, self.TIGER)
        self.TIGER_domestic = self.exceptCodeName(bondWords + foreignWords + inverseWords + productWords, self.TIGER)
        self.TIGER_exceptProduct = self.exceptCodeName(productWords + foreignWords, self.TIGER)
        self.TIGER_inverse = self.chooseCodeName(inverseWords, self.TIGER)
        self.TIGER_product = self.chooseCodeName(inverseWords, self.TIGER)

        #KOSEF 종목 (키움)
        self.KOSEF = self.exceptCodeName(blackWords, self.KOSEF)
        self.KOSEF_bond = self.chooseCodeName(bondWords, self.KOSEF)
        self.KOSEF_foreign = self.chooseCodeName(foreignWords, self.KOSEF)
        self.KOSEF_domestic = self.exceptCodeName(bondWords + foreignWords + inverseWords + productWords, self.KOSEF)
        self.KOSEF_exceptProduct = self.exceptCodeName(productWords + foreignWords, self.KOSEF)
        self.KOSEF_inverse = self.chooseCodeName(inverseWords, self.KOSEF_exceptProduct)
        self.KOSEF_product = self.chooseCodeName(productWords, self.KOSEF)

        return self.TIGER, self.KODEX, self.KODEX


    def loadTopDf(self):
        topcap = self.load(self.makeName('TOPCAP', '2007-01-01', '2019-12-31'))
        #시가
        targetShares = {}
        for index, row  in topcap.iterrows():
            targetShares[row['Code']] = row['Name']
        topcapdf = self.loadStockFromDict(self.makeName('SHARETOPCAP', beforeStr='2006-01-01', endDateStr='2019-07-29'), targetShares, '2005-12-31', '2019-07-29')
        self.filterETF()
        etfdf = self.loadStockFromArr(self.makeName('ETF2', beforeStr='2006-12-31', endDateStr='2019-12-31'), self.KODEX + self.TIGER + self.KOSEF, '2006-12-31', '2019-07-29')
        etfdf.index = etfdf.index.map(lambda dt: pd.to_datetime(dt.date()))
        topdf = pd.concat([etfdf,topcapdf['close']], sort=False, axis=1)

        return topdf, topcap

    def loadStockDf(self):
        with ThreadPoolExecutor(5) as executor:
            future1 = executor.submit(self.load,self.makeName('TOPCAP', '2007-01-01', '2019-12-31'))
            future2 = executor.submit(self.filterETF)
            # topcap = self.load(self.makeName('TOPCAP', '2007-01-01', '2019-12-31'))
            # self.filterETF()
            self.TIGER, self.KODEX, self.KODEX = future2.result()
            topcap = future1.result()

            domesticTargets = [ {'Code':row['Code'], 'Name':row['Name']} for index, row  in topcap.iterrows()]
            etfTargets = self.KODEX + self.TIGER + self.KOSEF
            stockdf = pd.DataFrame(columns=['날짜','종목명', '종목코드', '종가', '시가', '고가', '저가', '거래량'])
            domesticName = self.makeName('SHARETOPCAP', beforeStr='2006-01-01', endDateStr='2019-12-31')
            etfName = self.makeName('ETF2', beforeStr='2006-12-31', endDateStr='2019-12-31')
            
            future3 = executor.submit(self.loadStockDataFrame,domesticName, domesticTargets, stockdf, '2006-01-01', '2019-12-31')
            future4 = executor.submit(self.loadStockDataFrame,etfName, etfTargets, stockdf, '2006-01-01', '2019-12-31')
            stockdf1 = future3.result()
            stockdf2 = future4.result()
            stockdf = pd.concat([stockdf1, stockdf2], axis=0)
            # stockdf = self.loadStockDataFrame(domesticName, domesticTargets, stockdf, '2006-01-01', '2019-12-31')
            # stockdf = self.loadStockDataFrame(etfName, etfTargets, stockdf, '2006-01-01', '2019-12-31')

        return stockdf, topcap

    
    def loadStockDataFrame(self, name, targets, stockdf, beforeStr, endStr):
        tempstockdf = pd.DataFrame(columns=['날짜','종목명', '종목코드', '종가', '시가', '고가', '저가', '거래량'])
        if not os.path.isfile(name):
            date = NaverDate.create(startDate=beforeStr, endDate=endStr)
            progress = 0
            compliteLen = len(targets)
            for target in targets:
                print(target['Name'],'collect...', str(progress),'/',str(compliteLen) ,str(progress/compliteLen)+'%')
                crawler = NaverStockCrawler.create(target['Code'])
                data = crawler.crawling(date)
                for result in data:
                    priceDate = pd.to_datetime(result.date, format='%Y-%m-%d')
                    resultDict = { \
                        '날짜': priceDate, \
                        '종목명': target['Name'], \
                        '종목코드': target['Code'], \
                        '종가': result.close, \
                        '시가': result.open, \
                        '고가': result.high, \
                        '저가': result.low, \
                        '거래량': result.volume \
                    }
                    tempstockdf = tempstockdf.append(resultDict, ignore_index=True)
                progress+=1
            tempstockdf.to_hdf(name, key='df', mode='w')
        else:
            print(name, 'read...')
            tempstockdf = pd.read_hdf(name, key='df')
        # stockdf = pd.concat([stockdf, tempstockdf], axis=0)
        return tempstockdf



    
    def loadStockFromDict(self, name, targets, beforeStr, endStr):
        close = dict()
        open_ = dict()
        high = dict()
        low = dict()
        volume = dict()
        if not os.path.isfile(name):
            date = NaverDate.create(startDate=beforeStr, endDate=endStr)
            progress = 0
            compliteLen = len(targets.keys())
            for key in targets:
                print(targets[key],'collect...', str(progress),'/',str(compliteLen) ,str(progress/compliteLen)+'%')
                crawler = NaverStockCrawler.create(key)
                data = crawler.crawling(date)

                close[key] = { pd.to_datetime(item.date, format='%Y-%m-%d') : item.close for item in data }
                open_[key] = { pd.to_datetime(item.date, format='%Y-%m-%d') : item.open for item in data }
                high[key] = { pd.to_datetime(item.date, format='%Y-%m-%d') : item.high for item in data }
                low[key] = { pd.to_datetime(item.date, format='%Y-%m-%d') : item.low for item in data }
                volume[key] = { pd.to_datetime(item.date, format='%Y-%m-%d') : item.volume for item in data }
                progress+=1
            topdf = {'close': pd.DataFrame(close), 'open': pd.DataFrame(open_), 'high': pd.DataFrame(high), 'low': pd.DataFrame(low), 'volume': pd.DataFrame(volume)}
            topdf['close'].to_hdf(name, key='df', mode='w')
            topdf['open'].to_hdf('open_'+name, key='df', mode='w')
            topdf['high'].to_hdf('high_'+name, key='df', mode='w')
            topdf['low'].to_hdf('low_'+name, key='df', mode='w')
            topdf['volume'].to_hdf('volume_'+name, key='df', mode='w')
        else:
            print(name, 'read...')
            close = pd.read_hdf(name, key='df')
            open_ = pd.read_hdf('open_'+name, key='df')
            high = pd.read_hdf('high_'+name, key='df')
            low = pd.read_hdf('low_'+name, key='df')
            volume = pd.read_hdf('volume_'+name, key='df')
            topdf = {'close': close, 'open': open_, 'high': high, 'low': low, 'volume': volume}
        return topdf
    
    def loadStockFromArr(self, name, targets, beforeStr, endStr):
        prices = dict()
        if not os.path.isfile(name):
            date = NaverDate.create(startDate=beforeStr, endDate=endStr)
            for target in targets:
                print(target['Name'], 'collect...')
                crawler = NaverStockCrawler.create(target['Code'])
                data = crawler.crawling(date)
                prices[target['Name']] = { pd.to_datetime(item.date, format='%Y-%m-%d') : item.close for item in data }
            bonddf = pd.DataFrame(prices)
            bonddf.to_hdf(name, key='df', mode='w')
        else:
            print(name, 'read...')
            bonddf = pd.read_hdf(name, key='df')
        return bonddf
    
    def loadDomesticIndex(self, name, beforeStr, endStr):
        if not os.path.isfile(name):
            print(name, 'collect...')
            crawler = NaverCrawler.create(targetName=name.split('_')[0])
            date = NaverDate.create(startDate=beforeStr, endDate=endStr)
            data = crawler.crawling(dateData=date)
            df = pd.DataFrame(columns=['종가', '전일비', '등락률', '거래량', '거래대금'])
            for v in data:
                df.loc[v.index()] = v.value()
            df.to_hdf(name, key='df', mode='w')
        else:
            print(name, 'read...')
            df = pd.read_hdf(name, key='df')
        return df
    def loadDartData(self, topcap):
        name = 'DART_'+ '2007-01-01_2019-12-31.h5'
        if not os.path.isfile(name):
            targets = []
            for index, row  in topcap.iterrows():
                value = {'Code': row['Code'], 'Name': row['Name']}
                if not value in targets:
                    targets.append(value)
            df = pd.DataFrame(columns=['종목코드','종목명','당기순이익', '자본총계', '주식수', '시가총액'])
            for target in targets:
                print(target)
                dartCrawler = DartCrawler.create(target, '2007-01-01', '2019-12-31')
                data = dartCrawler.crawling()
                if not data:
                    continue
                newdf = pd.DataFrame(columns=['종목코드','종목명','당기순이익', '자본총계', '주식수', '시가총액'])
                for v in data:
                    date = pd.to_datetime(v['date'], format='%Y.%m')
                    marcap = topcap[topcap['Code']==target['Code']]
                    if date.year in marcap.index.year:
                        marcap = marcap[str(date.year)]['Marcap'].values[0]
                    else:
                        marcap = None
                    #append로 변경해야함
                    newdf.loc[date] = [v['Code'], v['Name'],v['profit'], v['total'], v['stockNum'], marcap]
                print(newdf)
                time.sleep(0.5)
                df = pd.concat([df,newdf])
                # values[target['Name']] = { pd.to_datetime(item.date, format='%Y-%m-%d') : item.close for item in data }
            df.to_hdf(name, key='df', mode='w')
        else:
            print(name, 'read...')
            df = pd.read_hdf(name, key='df')
        return df
    
    def loadSearchCodeName(self, name):
        crawler = NavarSearchCodeCrawler.create(name)
        return crawler.crawling()

    def chooseCodeName(self, filterList, items):
        return [ {'Name':t['Name'], 'Code':t['Code']} for word in filterList for t in filter(lambda x : x['Name'].find(word) >= 0, items)]
    
    def exceptCodeName(self, filterList, items):
        targetList = []
        for target in items:
            isIn = False
            for word in filterList:
                if target['Name'].find(word) > 0:
                    isIn = True
                    break
            if not isIn:
                targetList.append({'Name':target['Name'], 'Code':target['Code']})

        return targetList
    
    def loadFactor(self):
        upCodes = ['제조업']
        factors = ['per', 'pcr', 'pbr', 'roe', '당기순이익', '영업활동으로인한현금흐름', '투자활동으로인한현금흐름', '재무활동으로인한현금흐름', 'psr', 'roic', 'eps', 'ebit', 'ev_ebit', 'ev_sales', 'ev_ebitda', '당기순이익률', '영업이익률', '매출총이익률', '배당수익률', '매출액', '자산', '유동자산', '부채', '유동부채', '이익잉여금']
        dfs = {}
        for upCode in upCodes:
            for factor in factors:
                name = 'finData/'+upCode+'_'+factor+'.xlsx'
                df = pd.read_excel(name,sheet_name='계정별 기업 비교 - 특정계정과목', skiprows=8)
                df.columns = list(df.iloc[0])
                df = df.drop([0])
                df = df.set_index("종목코드")
                if factor not in dfs:
                    dfs[factor] = pd.DataFrame()
                dfs[factor] = pd.concat([dfs[factor], df])
        return dfs
