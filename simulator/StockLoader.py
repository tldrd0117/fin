import os
import pandas as pd
import time
import numpy as np
import json

from crawler.naver.NaverTopMarketCapCrawler import NaverTopMarketCapCrawler
from crawler.naver.data.NaverDate import NaverDate
from crawler.naver.NaverStockCrawler import NaverStockCrawler
from crawler.naver.DartCrawler import DartCrawler
from crawler.naver.NavarSearchCodeCrawler import NavarSearchCodeCrawler
from crawler.naver.NaverCrawler import NaverCrawler
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
# from simulator.MongoStockCollection import MongoStockCollection

pd.set_option('display.float_format', None)
class StockLoader:
    @staticmethod
    def create():
        stockloader = StockLoader()
        stockloader.filterETF()
        return stockloader
    
    def makeName(self, name, beforeStr, endDateStr):
        return 'h5data/'+name + '_' + beforeStr + '_' + endDateStr + '.h5'

    def makeNameJson(self, name, beforeStr, endDateStr):
        return 'h5data/'+name + '_' + beforeStr + '_' + endDateStr + '.json'

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

    # def loadStockFromDb(self):
    #     msc = MongoStockCollection.create()
    #     stockDb = msc.get()
    #     stockDb.find()

    def loadTopDf(self):
        pd.options.display.float_format = '{:.2f}'.format
        topcap = self.load(self.makeName('TOPCAP', '2007-01-01', '2019-12-31'))
        #5천억 500000000000
        #300억 30000000000
        allCodes = {}
        allNames = {}
        for index, row  in topcap.iterrows():
            allCodes[row['Code']] = row['Name']
            allNames[row['Name']] = row['Code']
        topcap = topcap[topcap['Marcap']<=700000000000]
        #시가
        targetShares = {}
        reversedCode = {}
        for index, row  in topcap.iterrows():
            targetShares[row['Code']] = row['Name']
            reversedCode[row['Name']] = row['Code']
        shareTopCapName = self.makeName('SHARETOPCAP', beforeStr='2006-01-01', endDateStr='2019-12-31')
        etfName = self.makeName('ETF2', beforeStr='2006-12-31', endDateStr='2019-12-31')
        topcapdf = self.loadStockFromDict(shareTopCapName, targetShares, '2005-12-31', '2019-12-31')
        etfdf = self.loadStockFromArr(etfName, self.KODEX + self.TIGER + self.KOSEF, '2006-12-31', '2019-12-31')
        etfdf.index = etfdf.index.map(lambda dt: pd.to_datetime(dt.date()))
        topdf = pd.concat([etfdf,topcapdf], sort=False, axis=1)
        
        return topdf, topcap, allCodes, allNames
    
    def loadTopcapDf(self, maxMarketCap=700000000000, minMarketCap=0):
        pd.options.display.float_format = '{:.2f}'.format
        topcap = self.load(self.makeName('TOPCAP', '2007-01-01', '2020-04-12'))
        #5천억 500000000000
        #300억 30000000000
        topcap = topcap[topcap['Marcap']<=maxMarketCap]
        topcap = topcap[topcap['Marcap']>=minMarketCap]
        allCodes = {}
        allNames = {}
        for index, row  in topcap.iterrows():
            allCodes[row['Code']] = row['Name']
            allNames[row['Name']] = row['Code']
        return topcap, allCodes, allNames
    
    def loadTopLately(self,topcap, start, end):
        pd.options.display.float_format = '{:.2f}'.format
        #5천억 500000000000
        #300억 30000000000
        #시가
        targetShares = {}
        reversedCode = {}
        for index, row  in topcap.iterrows():
            targetShares[row['Code']] = row['Name']
            reversedCode[row['Name']] = row['Code']
        topcapdf = self.loadStockFromDict(self.makeName('SHARETOPCAP', beforeStr=start, endDateStr=end), targetShares, start, end)
        etfdf = self.loadStockFromArr(self.makeName('ETF2', beforeStr=start, endDateStr=end), self.KODEX + self.TIGER + self.KOSEF, start, end)
        etfdf.index = etfdf.index.map(lambda dt: pd.to_datetime(dt.date()))
        topdf = pd.concat([etfdf,topcapdf], sort=False, axis=1)
        return topdf
    
    def loadMarcap(self):
        marcapdf = self.load('h5data/MARCAP_2006-01-01_2019_05_30.h5')
        # indexes = list(set(marcapdf.index.values))
        # columns = list(set(marcapdf['Code'].values))
        # topdf = pd.DataFrame([], index=indexes, columns=columns)
        # for idx in indexes:
        #     for column in columns:
        #         marcap = marcapdf.loc[idx]
        #         close = marcap[marcap['Code']==column]['Close'].values
        #         if len(close) > 0:
        #             topdf.at[idx, column] = marcap[marcap['Code']==column]['Close'].values[0]
        #     print(topdf)
        return marcapdf
    def loadAmountDf(self):
        marcapdf = self.load('h5data/MARCAP_2006-01-01_2019_05_30.h5')
        indexes = list(set(marcapdf.index.values))
        columns = list(set(marcapdf['Code'].values))
        topdf = pd.DataFrame([], index=indexes, columns=columns)
        for idx in indexes:
            for column in columns:
                marcap = marcapdf.loc[idx]
                close = marcap[marcap['Code']==column]['Close'].values
                if len(close) > 0:
                    topdf.at[idx, column] = marcap[marcap['Code']==column]['Close'].values[0]
            print(topdf)

    
    def loadCodeName(self):
        codeName = {}
        for year in range(2007,2020):
            kospiCompanyDf = pd.read_excel('finData/시가총액_'+year+'.xlsx',sheet_name='시가총액', skiprows=3, converters={'종목코드':str})
            kospiCompanyDf = kospiCompanyDf.iloc[1:]
            for index, row in kospiCompanyDf.iterrows():
                codeName[row['종목코드']] = row['종목명']
        return codeName
    
    def loadTermTopDf(self, codeName, start, end):
        name = self.makeName('SHARETOPCAP', beforeStr=start, endDateStr=end)
        self.filterETF()
        topcapdf = self.loadStockFromDict(name, codeName, start, end)
        
        pass
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
            domesticName = self.makeName('SHARETOPCAP3', beforeStr='2006-01-01', endDateStr='2019-12-31')
            etfName = self.makeName('ETF3', beforeStr='2006-01-01', endDateStr='2019-12-31')
            
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

    def loadStockJson(self, name, targets, beforeStr, endStr):
        dataArray = []
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
                    dataArray.append(resultDict)
                progress+=1
            
            with open(name, 'w') as fp:
                json.dump(dataArray, fp)
        else:
            print(name, 'is Already Exist')
        return dataArray
        # stockdf = pd.concat([stockdf, tempstockdf], axis=0)
    def loadStockMongo(self, name, targets, beforeStr, endStr, stockdb):
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
                # stockdb.update({'날짜':priceDate,'종목명':target['Name']}, resultDict, upsert=True, multi=False)
                stockdb.insert(resultDict)
            progress+=1
        print('complete')
        # return list(stockdb.find())


    def loadStockFromDict(self, name, targets, beforeStr, endStr):
        prices = dict()
        if not os.path.isfile(name):
            date = NaverDate.create(startDate=beforeStr, endDate=endStr)
            progress = 0
            compliteLen = len(targets.keys())
            for key in targets:
                print(targets[key],'collect...', str(progress),'/',str(compliteLen) ,str(progress/compliteLen*100)+'%')
                crawler = NaverStockCrawler.create(key)
                data = crawler.crawling(date)

                prices[targets[key]] = { pd.to_datetime(item.date, format='%Y-%m-%d') : item.close for item in data }
                progress+=1

            topdf = pd.DataFrame(prices)
            topdf.to_hdf(name, key='df', mode='w')
        else:
            print(name, 'read...')
            topdf = pd.read_hdf(name, key='df')
        return topdf

    def loadStockAllFromDict(self, name, targets, beforeStr, endStr):
        prices = dict()
        if not os.path.isfile(name):
            date = NaverDate.create(startDate=beforeStr, endDate=endStr)
            progress = 0
            compliteLen = len(targets.keys())
            for key in targets:
                print(targets[key],'collect...', str(progress),'/',str(compliteLen) ,str(progress/compliteLen)+'%')
                crawler = NaverStockCrawler.create(key)
                data = crawler.crawling(date)

                prices[targets[key]] = { pd.to_datetime(item.date, format='%Y-%m-%d') : item.close for item in data }
                progress+=1

            topdf = pd.DataFrame(prices)
            topdf.to_hdf(name, key='df', mode='w')
        else:
            print(name, 'read...')
            topdf = pd.read_hdf(name, key='df')
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
    
    def loadFactor(self, year=2018):
        upCodes = ['제조업']
        factors = ['당기순이익','영업활동으로인한현금흐름','투자활동으로인한현금흐름', '재무활동으로인한현금흐름','당기순이익률', '영업이익률', '매출총이익률', '배당수익률', '매출액', '자산', '유동자산', '부채', '유동부채', '이익잉여금', 'roe','ebit','eps']
        #factors = ['per', 'pcr', 'pbr', 'roe', '당기순이익', '영업활동으로인한현금흐름', '투자활동으로인한현금흐름', '재무활동으로인한현금흐름', 'psr', 'roic', 'eps', 'ebit', 'ev_ebit', 'ev_sales', 'ev_ebitda', '당기순이익률', '영업이익률', '매출총이익률', '배당수익률', '매출액', '자산', '유동자산', '부채', '유동부채', '이익잉여금']
        dfs = {}
        for upCode in upCodes:
            for factor in factors:
                name = 'finData/'+str(year)+'/'+upCode+'_'+factor+'.xlsx'
                df = pd.read_excel(name,sheet_name='계정별 기업 비교 - 특정계정과목', skiprows=8)
                coli = list(df.iloc[0])
                for i in range(len(coli)):
                    if i >= 4:
                        coli[i] = float(coli[i])
                df.columns = coli
                df = df.drop([0])
                df = df.set_index("종목코드")
                if factor not in dfs:
                    dfs[factor] = pd.DataFrame()
                dfs[factor] = pd.concat([dfs[factor], df])
        return dfs
    def loadQuaterFactor(self):
        upCodes = ['제조업']
        factors = ['당기순이익', '영업활동으로인한현금흐름', 'ebit', '당기순이익률', '영업이익률', '매출액', '자산', '유동자산', '유동부채', '이익잉여금']
        dfs = {}
        for upCode in upCodes:
            for factor in factors:
                name = 'finData/quater/'+upCode+'_'+factor+'.xlsx'
                df = pd.read_excel(name,sheet_name='계정별 기업 비교 - 특정계정과목', skiprows=8)
                df.columns = list(df.iloc[0])
                df = df.drop([0])
                df = df.set_index("종목코드")
                if factor not in dfs:
                    dfs[factor] = pd.DataFrame()
                dfs[factor] = pd.concat([dfs[factor], df])
        return dfs