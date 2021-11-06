
# In[0]:
from urllib import request
from zipfile import ZipFile
apiKey = "48a43d39558cf752bc8d8e52709da34569a80372"
url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={apiKey}"
savename = "findata/2020/codes.zip"
loadname = "findata/2020/codes"

request.urlretrieve(url,savename)
ZipFile(file=savename).extractall(loadname)


# In[1]:

#<list>
#     <corp_code>00434003</corp_code>
#     <corp_name>다코</corp_name>
#     <stock_code> </stock_code>
#     <modify_date>20170630</modify_date>
# </list>
import xml.etree.ElementTree as ET
datapath = "findata/2020/codes/CORPCODE.xml"
tree = ET.parse(datapath)
codes = {}
for li in tree.findall("list"):
    stockCode = li.find("stock_code").text
    if len(stockCode) == 6:
        codes[stockCode] = li.find("corp_code").text

print(codes)
print(len(codes.keys()))
# tree.findall("list/[stock_code='005930']")[0].find("corp_code").text


# In[2]:
import urllib3
import json
import pandas as pd
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) 
http = urllib3.PoolManager()

# 재무상태표
# ["자산","유동자산","부채","유동부채","이익잉여금","자본총계"]
# 손익계산서
# ['영업이익', '매출액', "매출총이익"]
# 현금흐름표
# ["영업활동으로 인한 현금흐름", "투자활동으로 인한 현금흐름", "재무활동으로 인한 현금흐름","당기순이익","이자비용","법인세비용"]

codes = [{"자산":"ifrs-full_Assets"},
        {"유동자산":"ifrs-full_CurrentAssets"},
        {"부채":"ifrs-full_Liabilities"},
        {"유동부채":"ifrs-full_CurrentLiabilities"},
        {"이익잉여금":"ifrs-full_RetainedEarnings"},
        {"자본총계":"ifrs-full_Equity"},
        {"영업이익":"dart_OperatingIncomeLoss"},
        {"매출액":"ifrs-full_Revenue"},
        {"매출총이익":"ifrs-full_GrossProfit"},
        {"영업활동으로인한현금흐름":"ifrs-full_CashFlowsFromUsedInOperatingActivities"},
        {"투자활동으로인한현금흐름":"ifrs-full_CashFlowsFromUsedInInvestingActivities"},
        {"재무활동으로인한현금흐름":"ifrs-full_CashFlowsFromUsedInFinancingActivities"},
        {"당기순이익":"ifrs-full_ProfitLoss"},
        {"이자비용":"ifrs-full_InterestPaidClassifiedAsOperatingActivities"},
        {"법인세비용":"ifrs-full_IncomeTaxExpenseContinuingOperations"}
        ]

url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json?"
url = url + "crtfc_key=" + apiKey
url = url + "&corp_code=00126380"
url = url + "&bsns_year=2020"
url = url + "&reprt_code=11011"
url = url + "&fs_div=OFS"
print(url)
r = http.request('GET', url, timeout=10, retries=10, headers={'User-Agent':'Mozilla/5.0'})

data = r.data.decode("utf-8")
result = json.loads(data)
columns = ["code","자산","유동자산","부채","유동부채","이익잉여금","자본총계","영업이익","매출액","매출총이익","영업활동으로인한현금흐름","투자활동으로인한현금흐름","재무활동으로인한현금흐름","당기순이익","이자비용","법인세비용"]
df = pd.DataFrame(columns=columns)

resultDict=dict()
resultDict["code"] = "005930"
for item in result["list"]:
    for code in codes:
        key = list(code.keys())[0]
        if item["account_id"] == code[key]:
            resultDict[key] = item["thstrm_amount"]
print(resultDict)
print(df)
resultDict["매출총이익률"] = float(resultDict["매출총이익"]) / float(resultDict["매출액"]) * 100
resultDict["영업이익률"] = float(resultDict["영업이익"]) / float(resultDict["매출액"]) * 100
resultDict["당기순이익률"] = float(resultDict["당기순이익"]) / float(resultDict["매출액"]) * 100
resultDict["roe"] = float(resultDict["당기순이익"]) / float(resultDict["자본총계"]) * 100
resultDict["ebit"] = str(float(resultDict["당기순이익"]) + float(resultDict["이자비용"]) + float(resultDict["법인세비용"]))
df = df.append(resultDict, ignore_index=True)
df

# 매출총이익률: 매출총이익 / 매출액 * 100
# 영업이익률: 영업이익 / 매출액 * 100
# 당기순이익률: 당기순이익 / 매출액 * 100
# roe: 당기순이익/자본총계
# ebit: 당기순이익+이자비용+법인세비용

# with open("findata/2020/testJson.json", 'w') as fi:
#     fi.write(r.data.decode("utf-8"))

# In[3]:
from crawler.opendart.DartFactorCrawler import DartFactorCrawler
crawler = DartFactorCrawler()
crawler.getFactors("2020")

# In[4]:
import pandas as pd
df = pd.read_hdf("h5data/FACTORS_2019-12-30.h5")
def isNumber(val):
    return isinstance(val, (int, float, complex))
def isnum(s):
    try:
        float(s)
    except:
        return(False)
    else:
        return(True)
def toNumeric(df):
    return df.applymap(lambda v : float(v) if isNumber(v) or isnum(v) else np.nan )
# df = toNumeric(df)
df[df["영업활동으로인한현금흐름"]=="0"]

# df.to_csv(f"finData/2020/factor.csv")

# In[5]:
import pandas as pd
from simulator.StockStrategy import StockStrategy
ss = StockStrategy.create()
factorDartDf = pd.read_hdf("h5data/FACTORS_2020-12-30-2.h5")
factorDartDf = factorDartDf.set_index("code")
factorDartDf[factorDartDf.index.isin(["005930"])]

# %%
