import os
import pandas as pd
import time

from crawler.naver.NaverTopMarketCapCrawler import NaverTopMarketCapCrawler
from crawler.naver.data.NaverDate import NaverDate
from crawler.naver.NaverStockCrawler import NaverStockCrawler
from crawler.naver.DartCrawler import DartCrawler
from crawler.naver.NavarSearchCodeCrawler import NavarSearchCodeCrawler
from crawler.naver.NaverCrawler import NaverCrawler

class StockLoader:
    @staticmethod
    def create():
        stockloader = StockLoader()
        return stockloader
    
    def makeName(self, name, beforeStr, endDateStr):
        return name + '_' + beforeStr + '_' + endDateStr + '.h5'

    def topk(self, num):
        crawler = NaverTopMarketCapCrawler.create()
        data = crawler.crawling([1,num])
        codes = [ {'code':item.code, 'name':item.name}  for item in data ]
        return codes

    def load(self, name):
        print(name, 'read...')
        return pd.read_hdf(name, key='df')
    
    def loadStockFromDict(self, name, targets, beforeStr, endStr):
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
    
    def loadFactor(self):
        upCodes = ['제조업', '은행업', '증권업', '보험업', '종합금융업', '여신전문금융업', '신용금고']
        factors = ['per', 'pcr', 'pbr', 'roe', '당기순이익', '영업활동으로인한현금흐름', '투자활동으로인한현금흐름', '재무활동으로인한현금흐름']
        dfs = {}
        for upCode in upCodes:
            for factor in factors:
                name = 'fin/'+upCode+'_'+factor+'.xlsx'
                df = pd.read_excel(name,sheet_name='계정별 기업 비교 - 특정계정과목', skiprows=8)
                df.columns = list(df.iloc[0])
                df = df.drop([0])
                df = df.set_index("종목코드")
                if factor not in dfs:
                    dfs[factor] = pd.DataFrame()
                dfs[factor] = pd.concat([dfs[factor], df])
        return dfs
