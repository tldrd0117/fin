#In[1]: 
import re
targets = ['자산']

with open("finData/text/2019_사업보고서_03_포괄손익계산서_20200407.txt", "r", encoding="euckr") as f:
    content = f.readlines()
print(content[0])
del content[0]
values = []
for line in content:
    lineList = list(filter(lambda x: len(x)>0,line.split('\t')))
    name = lineList[11].strip()
    if name not in values:
        print(name)
        values.append(name)
    # for i in range(0,13):
        # print(lineList[i])
    # break
            # print(re.sub('[\[\]]','',lineList[1]))
            # print(lineList[2])
            # print(lineList[7][0:4])
            # print(t)
            # print(re.sub(',','',lineList[12]))

#In[2]:
import re
# targets = ['당기순이익','영업활동으로인한현금흐름','투자활동으로인한현금흐름', '재무활동으로인한현금흐름','당기순이익률', '영업이익률', '매출총이익률', '배당수익률', '매출액', '자산', '유동자산', '부채', '유동부채', '이익잉여금']
# targets = ['당기순이익(손실)', "당기순이익", "(1) 당기순이익", "1. 당기순이익","가.당기순이익","(1)당기순이익(손실).","1.당기순이익(손실)"]
targets = ["수익", "당기", "순이익", "순손실"]
with open("finData/text/2019_사업보고서_04_현금흐름표_20200407.txt", "r", encoding="euckr", errors="ignore") as f:
    content = f.readlines()
columns = set([])
names = set([])
for line in content:
    lineList = list(filter(lambda x: len(x)>0,line.split('\t')))
    for t in targets:
        if t in lineList[11].strip():
            val = lineList[1]+" "+lineList[11].strip()
            isSame = False
            if len(columns) > 0:
                for q in columns:
                    if val[0:8] == q.strip()[0:8]:
                        isSame = True
                        break
                if not isSame:
                    columns.add(val)
            else:
                columns.add(val)
    names.add(lineList[1])
print(len(columns))
print(len(names))
print(columns)
# columns
# %%
