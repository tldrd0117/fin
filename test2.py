# In[0]:
import pandas as pd
from simulator.StockLoader import StockLoader

df = pd.read_csv('finData/2020/2020_사업보고서_01_재무상태표_20210410.csv', sep=r',', delimiter=',', skipinitialspace=True)
# df.loc[df["항목명"].isin(["자산총계"])]
# df.loc[df["항목명"].isin(["자산총계"])].shape[0]
# df.shape[0]
# df = df[df["결산월"]==12]
df = df[df["결산기준일"]=="2020-12-31"]

def searchCode(df, code):
    newDf = None
    newDf = df[df['항목코드']==code]
    count = newDf.shape[0]
    print(f"count:{count}")
    dupCount = newDf[newDf["종목코드"].duplicated()].shape[0]
    print(f"dupCount:{dupCount}")
    return newDf

codes = [{"자산총계":"ifrs-full_Assets"},
        {"유동자산":"ifrs-full_CurrentAssets"},
        {"부채총계":"ifrs-full_Liabilities"},
        {"유동부채":"ifrs-full_CurrentLiabilities"},
        {"이익잉여금":"ifrs-full_RetainedEarnings"},
        {"자본총계":"ifrs-full_Equity"}]
# arr = ["자산총계","유동자산","부채총계","유동부채",["이익잉여금","이익잉여금(결손금)"],"자본총계"]
# for val in arr:
#     search(df, val)
for val in codes:
    print(val)
    key = list(val.keys())[0]
    df = searchCode(df,val[key]).to_csv(f"finData/2020/{key}.csv")


# df3 = df[df['항목명']=="유동자산"] #2156 2107
# print(df3[df3["종목코드"].duplicated()].shape[0])
# df[df['항목명'].str.contains("자산총계")].to_csv("finData/2020/test.csv")

# 재무상태표
# ["자산","유동자산","부채","유동부채","이익잉여금","자본총계"]
# 손익계산서
# ['영업이익', '매출액', "매출총이익"]
# 현금흐름표
# ["영업활동으로 인한 현금흐름", "투자활동으로 인한 현금흐름", "재무활동으로 인한 현금흐름","당기순이익","이자비용","법인세비용"]

# 매출총이익률: 매출총이익 / 매출액 * 100
# 영업이익률: 영업이익 / 매출액 * 100
# 당기순이익률: 당기순이익 / 매출액 * 100
# roe: 당기순이익/자본총계
# ebit: 당기순이익+이자비용+법인세비용

# %%
