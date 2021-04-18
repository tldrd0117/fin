
from urllib import request
from zipfile import ZipFile
import xml.etree.ElementTree as ET
import urllib3
import json
import pandas as pd
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) 
http = urllib3.PoolManager()

class DartFactorCrawler:
    #apiKey = "48a43d39558cf752bc8d8e52709da34569a80372"
    apiKey = "c211a43e6a9af3078ba60fc66708a51523a16bbf"
    url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={apiKey}"
    
    codeZipName = "findata/2020/codes.zip"
    coadZipExtractName = "findata/2020/codes"
    codeDataName = "findata/2020/codes/CORPCODE.xml"
    csvResultName = "finData/2020/factors.csv"
    hdfResultName = "h5data/FACTORS_2020-12-30.h5"

    codes = [{"자산":"ifrs-full_Assets"},
        {"유동자산":"ifrs-full_CurrentAssets"},
        {"부채":"ifrs-full_Liabilities"},
        {"유동부채":"ifrs-full_CurrentLiabilities"},
        {"이익잉여금":"ifrs-full_RetainedEarnings"},
        {"자본총계":"ifrs-full_Equity"},
        {"영업이익":"dart_OperatingIncomeLoss"},
        {"매출액":["ifrs-full_Revenue","ifrs-full_GrossProfit","매출액","영업수익"]},
        {"매출총이익":["ifrs-full_GrossProfit"]},
        {"영업활동으로인한현금흐름":"ifrs-full_CashFlowsFromUsedInOperatingActivities"},
        {"투자활동으로인한현금흐름":"ifrs-full_CashFlowsFromUsedInInvestingActivities"},
        {"재무활동으로인한현금흐름":"ifrs-full_CashFlowsFromUsedInFinancingActivities"},
        {"당기순이익":"ifrs-full_ProfitLoss"},
        {"이자비용":["ifrs-full_InterestPaidClassifiedAsOperatingActivities","ifrs-full_InterestPaidClassifiedAsFinancingActivities"]},
        {"법인세비용":"ifrs-full_IncomeTaxExpenseContinuingOperations"}
        ]

    columns = ["code","자산","유동자산","부채","유동부채","이익잉여금","자본총계","영업이익","매출액","매출총이익","영업활동으로인한현금흐름","투자활동으로인한현금흐름","재무활동으로인한현금흐름","당기순이익","이자비용","법인세비용","매출총이익률","영업이익률","당기순이익률","roe","ebit"]

    @staticmethod
    def create():
        return DartFactorCrawler()
    
    def extractCodes(self):
        request.urlretrieve(self.url, self.codeZipName)
        ZipFile(file=self.codeZipName).extractall( self.coadZipExtractName)
    
    def getCodeCorpsDict(self):
        tree = ET.parse(self.codeDataName)
        codes = {}
        for li in tree.findall("list"):
            stockCode = li.find("stock_code").text
            if len(stockCode) == 6:
                codes[stockCode] = li.find("corp_code").text
        return codes
    
    def getFactors(self, year):
        codeDict = self.getCodeCorpsDict()
        df = pd.DataFrame(columns=self.columns)
        index = 0
        length = len(list(codeDict.keys()))
        try:
            for key in codeDict:
                resultDict = self.getFactor(year, key, codeDict)
                if resultDict == None:
                    continue
                df = df.append(resultDict, ignore_index=True)
                index+=1
                print(f"{key} {str(index)} / {str(length)}")
            df.to_csv(self.csvResultName)
            df.to_hdf(self.hdfResultName, key='df', mode='w')
            print("complete!!")
        except Exception as e:
            print(f"error: {e}")
        
    
    def getFactor(self, year, stock_code, codeDict):
        corp_code = codeDict[stock_code]
        url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json?"
        url = url + "crtfc_key=" + self.apiKey
        url = url + f"&corp_code={corp_code}"
        url = url + f"&bsns_year={year}"
        url = url + "&reprt_code=11011"
        url = url + "&fs_div=OFS"
        r = http.request('GET', url, timeout=10, retries=10, headers={'User-Agent':'Mozilla/5.0'})
        data = r.data.decode("utf-8")
        result = json.loads(data)
        if not "list" in result:
            return None
        resultDict = dict()
        resultDict["code"] = stock_code
        for item in result["list"]:
            for code in self.codes:
                key = list(code.keys())[0]
                if isinstance(code[key], list):
                    if item["account_id"] in code[key]:
                        if not key in resultDict:
                            resultDict[key] = item["thstrm_amount"]
                    elif item["account_nm"] in code[key]:
                        if not key in resultDict:
                            resultDict[key] = item["thstrm_amount"]
                else:
                    if item["account_id"] == code[key]:
                        if not key in resultDict:
                            resultDict[key] = item["thstrm_amount"]
                    elif item["account_nm"] in code[key]:
                        if not key in resultDict:
                            resultDict[key] = item["thstrm_amount"]
        for key in self.columns:
            if not key in resultDict or resultDict[key] == "" or not resultDict[key]:
                resultDict[key] = "0"
        if float(resultDict["매출액"]) != 0:
            resultDict["매출총이익률"] = float(resultDict["매출총이익"]) / float(resultDict["매출액"]) * 100
            resultDict["영업이익률"] = float(resultDict["영업이익"]) / float(resultDict["매출액"]) * 100
            resultDict["당기순이익률"] = float(resultDict["당기순이익"]) / float(resultDict["매출액"]) * 100
        resultDict["roe"] = float(resultDict["당기순이익"]) / float(resultDict["자본총계"]) * 100
        resultDict["ebit"] = str(float(resultDict["당기순이익"]) + float(resultDict["이자비용"]) + float(resultDict["법인세비용"]))
        return resultDict

