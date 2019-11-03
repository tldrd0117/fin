import pandas as pd
import math
import numpy as np

class StockStrategy:
    @staticmethod
    def create():
        stockStrategy = StockStrategy()
        return stockStrategy
    
    def getCurrentFactor(self, current, factordf, targetdf, sName, factor):
        codeList = list(map(lambda x: sName[x], list(targetdf.columns)))
        yearDf = factordf[factor][factordf[factor].index.isin(codeList)]
       
        # yearDf = factordf[factor][factordf[factor].index.isin(targetdf.columns)]
        # print(factordf[factor])
        if current.month > 4:
            yearDf = yearDf[current.year - 1]
        else:
            yearDf = yearDf[current.year - 2]
        return yearDf

    def getFactor(self, current, factordf, factor, code, sName):
        codeList = list(map(lambda x: sName[x], list([code])))
        df = factordf[factor][factordf[factor].index.isin(codeList)]
        # df = factordf[factor][factordf[factor].index.isin([code])]
        if len(df.index) <= 0:
            return 'None'
        if current.month > 4:
            return df.loc[sName[code], current.year - 1]
        else:
            return df.loc[sName[code], current.year - 2]
    
    def getQuarterFactor(self, current, factordf, factor, code, sName):
        codeList = list(map(lambda x: sName[x], list([code])))
        df = factordf[factor][factordf[factor].index.isin(codeList)]
        # df = factordf[factor][factordf[factor].index.isin([code])]
        if len(df.index) <= 0:
            return 'None'
        elif current.month > 9:
            #반기보고서
            return df.loc[sName[code], float(str(current.year)+'.06')]
        elif current.month > 5:
            #1/4분기보고서
            return df.loc[sName[code], float(str(current.year)+'.03')]
        elif current.month > 4:
            #사업보고서
            return df.loc[sName[code], float(str(current.year-1)+'.12')]
        elif current.month > 0:
            #3/4분기보고서
            return df.loc[sName[code], float(str(current.year-1)+'.09')]

    def getCurrentQuarterFactor(self, current, factordf, targetdf, sName, factor):
        codeList = list(map(lambda x: sName[x], list(targetdf.columns)))
        df = factordf[factor][factordf[factor].index.isin(codeList)]
        # df = factordf[factor][factordf[factor].index.isin([code])]
        if len(df.index) <= 0:
            return 'None'
        elif current.month > 9:
            #반기보고서
            return df[float(str(current.year)+'.06')]
        elif current.month > 5:
            #1/4분기보고서
            return df[float(str(current.year)+'.03')]
        elif current.month > 4:
            #사업보고서
            return df[float(str(current.year-1)+'.12')]
        elif current.month > 0:
            #3/4분기보고서
            return df[float(str(current.year-1)+'.09')]

    def filterAltmanZScoreQuarter(self, current, targetdf, factordf, topcap, sName, sCode):
        floatingAsset = self.getCurrentQuarterFactor(current, factordf, targetdf, sName, '유동자산')
        floatingLiablilities = self.getCurrentQuarterFactor(current, factordf, targetdf, sName, '유동부채')
        totalAsset = self.getCurrentQuarterFactor(current, factordf, targetdf, sName, '자산')
        liablilities = self.getCurrentQuarterFactor(current, factordf, targetdf, sName, '자산')
        retainedEarning = self.getCurrentQuarterFactor(current, factordf, targetdf, sName, '이익잉여금')
        sales = self.getCurrentQuarterFactor(current, factordf, targetdf, sName, '매출액')
        ebit = self.getCurrentQuarterFactor(current, factordf, targetdf, sName, 'ebit')
        topcap = topcap.resample('A').first()
        loc = topcap.index.get_loc(current, method='pad')
        marketValueOfEquity = topcap.iloc[loc]['Marcap']
        
        x1 = (floatingAsset - floatingLiablilities) / totalAsset
        x2 = retainedEarning / totalAsset
        x3 = ebit / totalAsset
        x4 = marketValueOfEquity / liablilities
        x5 = sales / totalAsset
        altmanZ = 1.2 * x1 + 1.4 * x2 + 3.3 * x3 + 0.6 * x4 + 0.999 * x5
        return list(map(lambda x : sCode[x],list(altmanZ[altmanZ >= 1.81].index)))

    def filterAltmanZScore(self, current, targetdf, factordf, topcap, sName, sCode ):
        floatingAsset = self.getCurrentFactor(current, factordf, targetdf, sName, '유동자산')
        floatingLiablilities = self.getCurrentFactor(current, factordf, targetdf, sName, '유동부채')
        totalAsset = self.getCurrentFactor(current, factordf, targetdf, sName, '자산')
        liablilities = self.getCurrentFactor(current, factordf, targetdf, sName, '자산')
        retainedEarning = self.getCurrentFactor(current, factordf, targetdf, sName, '이익잉여금')
        sales = self.getCurrentFactor(current, factordf, targetdf, sName, '매출액')
        ebit = self.getCurrentFactor(current, factordf, targetdf, sName, 'ebit')
        topcap = topcap.resample('A').first()
        loc = topcap.index.get_loc(current, method='pad')
        marketValueOfEquity = topcap.iloc[loc]['Marcap']
        
        x1 = (floatingAsset - floatingLiablilities) / totalAsset
        x2 = retainedEarning / totalAsset
        x3 = ebit / totalAsset
        x4 = marketValueOfEquity / liablilities
        x5 = sales / totalAsset
        altmanZ = 1.2 * x1 + 1.4 * x2 + 3.3 * x3 + 0.6 * x4 + 0.999 * x5
        return list(map(lambda x : sCode[x],list(altmanZ[altmanZ >= 1.81].index)))
    
    def getMomentumScore(self, current, targetdf, mNum, mUnit):
        mdf = targetdf.resample('M').mean()
        beforeMomentumDate = current + pd.Timedelta(-mNum, unit=mUnit)
        start = mdf.index.get_loc(beforeMomentumDate, method='nearest')
        end = mdf.index.get_loc(current, method='nearest')
        
        oneYearDf = mdf.iloc[start:end+1]
        latelyValue = oneYearDf.iloc[-1]
        momentum = pd.DataFrame(latelyValue.values - oneYearDf.values, oneYearDf.index, oneYearDf.columns)
        # momentumScore = momentum.applymap(lambda val: 1 if val > 0 else 0 )
        momentumValues = momentum.values
        momentumValues[momentumValues>0] = 1
        momentumValues[momentumValues<=0] = 0
        momentumScore = pd.DataFrame(momentumValues, momentum.index, momentum.columns)
        return momentumScore
    
    def getMinusMomentumScore(self, current, targetdf, mNum, mUnit):
        mdf = targetdf.resample('M').mean()
        beforeMomentumDate = current + pd.Timedelta(-mNum, unit=mUnit)
        start = mdf.index.get_loc(beforeMomentumDate, method='pad')
        end = mdf.index.get_loc(current, method='pad')
        print(mdf.index[start], mdf.index[end], current)
        oneYearDf = mdf.iloc[start:end+1]
        # curVal = targetdf.index.get_loc(current, method='pad')
        # latelyValue = targetdf.iloc[curVal]
        latelyValue = oneYearDf.iloc[-1]
        momentum = pd.DataFrame(latelyValue.values - oneYearDf.values, oneYearDf.index, oneYearDf.columns)
        # momentumScore = momentum.applymap(lambda val: 1 if val > 0 else 0 )
        momentumValues = momentum.values
        momentumValues[momentumValues > 0] = 1
        momentumValues[momentumValues <= 0] = -1
        momentumScore = pd.DataFrame(momentumValues, momentum.index, momentum.columns)
        return momentumScore.mean(axis=0)
    
    def calculateMdf(self, current, targetdf, month):
        mdf = pd.DataFrame()
        dates = []
        for m in range(month + 1):
            date = current + pd.Timedelta(-m, 'M')
            dates.append(date)
        for i in range(len(dates) - 1):
            i1 = targetdf.index.get_loc(dates[i], method='pad')
            i2 = targetdf.index.get_loc(dates[i+1], method='pad')
            one = targetdf.iloc[i2:i1]
            oneMean = one.mean()
            newdf = pd.DataFrame([list(oneMean.values)], index=[one.index.values[-1]], columns=list(oneMean.index))
            mdf = pd.concat([mdf, newdf])
        return mdf, dates
    
    def getTimeDeltaMomentumScoreMonth(self, current, targetdf, month):
        mdf, dates = self.calculateMdf(current, targetdf, month)
        curVal = targetdf.index.get_loc(current, method='pad')
        latelyValue = targetdf.iloc[curVal - 1]
        momentum = pd.DataFrame(latelyValue.values - mdf.values, mdf.index, mdf.columns)
        colList = [ list(momentum.columns)[i:i+6] for i in range(0, len(momentum.columns), 6)]
        for cols in colList:
            print(momentum[cols])
        # momentumScore = momentum.applymap(lambda val: 1 if val > 0 else 0 )
        momentumValues = momentum.values
        momentumValues[momentumValues > 0] = 1
        momentumValues[momentumValues <= 0] = -1
        momentumScore = pd.DataFrame(momentumValues, momentum.index, momentum.columns)
        return momentumScore.mean(axis=0)
    
    def getTimeDeltaMomentumValueMonthExceptOneMonth(self, current, targetdf, month):
        mdf, dates = self.calculateMdf(current, targetdf, month)
        mdf = mdf[1:]
        curVal = targetdf.index.get_loc(current, method='pad')
        latelyValue = targetdf.iloc[curVal - 2]
        momentum = pd.DataFrame(latelyValue.values - mdf.values, mdf.index, mdf.columns)
        colList = [ list(momentum.columns)[i:i+6] for i in range(0, len(momentum.columns), 6)]
        for cols in colList:
            print(momentum[cols])
        # momentumScore = momentum.applymap(lambda val: 1 if val > 0 else 0 )
        momentumValues = (momentum/targetdf.iloc[curVal]).values
        momentumScore = pd.DataFrame(momentumValues, momentum.index, momentum.columns)
        return momentumScore.mean(axis=0)
    
    def getMomentumList(self, current, targetdf, mNum, mUnit, limit, minVal=-100, maxVal=100):
        momentumScore = self.getMinusMomentumScore(current, targetdf, mNum, mUnit)
        # momentumScore = momentumScore.query('')
        momentumScore = momentumScore[momentumScore >= minVal]
        momentumScore = momentumScore[momentumScore <= maxVal]
        sixMonthMomentumScore = self.getMinusMomentumScore(current, targetdf, 6, 'M')
        
        return list(momentumScore.sort_values(ascending=False).head(limit).index), sixMonthMomentumScore.sum()
    
    def getMomentumListMonthCurrent(self, current, targetdf, month, limit, minVal=-100, maxVal=100):
        momentumScore = self.getTimeDeltaMomentumScoreMonth(current, targetdf, month)
        # momentumScore = momentumScore.query('')
        momentumScore = momentumScore[momentumScore >= minVal]
        momentumScore = momentumScore[momentumScore <= maxVal]
        # sixMonthMomentumScore = self.getMinusMomentumScore(current, targetdf, 6, 'M')
        return list(momentumScore.sort_values(ascending=False).head(limit).index)
    
    def getMomentumListMonthExceptOneMonth(self, current, targetdf, month, limit, minVal=-100, maxVal=100):
        momentumScore = self.getTimeDeltaMomentumValueMonthExceptOneMonth(current, targetdf, month)
        # momentumScore = momentumScore.query('')
        print(momentumScore)
        momentumScore = momentumScore[momentumScore >= minVal]
        momentumScore = momentumScore[momentumScore <= maxVal]
        return list(momentumScore.sort_values(ascending=False).head(limit).index)
    
    def getMovingAvarage_20_big_5(self, current, targetdf):
        longTerm = current + pd.Timedelta(-21, unit='D')
        shortTerm = current + pd.Timedelta(-6, unit='D')
        lately = current + pd.Timedelta(-1, unit='D')
        term_20 = targetdf.loc[longTerm:lately].mean()
        term_5 = targetdf.loc[shortTerm:lately].mean()
        return list(term_5[term_20<=term_5].index)
    
    def getMovingAvarage_20_5_Break(self, current, targetdf):
        beforelongTerm = current + pd.Timedelta(-22, unit='D')
        beforeshortTerm = current + pd.Timedelta(-7, unit='D')

        longTerm = current + pd.Timedelta(-21, unit='D')
        shortTerm = current + pd.Timedelta(-6, unit='D')
        lately = current + pd.Timedelta(-1, unit='D')

        beforeTerm_20 = targetdf.loc[beforelongTerm:lately].mean()
        beforeTerm_5 = targetdf.loc[beforeshortTerm:lately].mean()

        term_20 = targetdf.loc[longTerm:lately].mean()
        term_5 = targetdf.loc[shortTerm:lately].mean()

        return list(term_5[(beforeTerm_20>beforeTerm_5)&(term_20<=term_5)].index)


    def isUnemployedYear(self, year):
        unemployedNum = [77.6, 89.4, 92.4, 86.3, 82.6, 80.8, 93.9, 97.6, 100.9, 102.3, 107.3, 122.4]
        idx = list(range(2008, 2020)).index(year)
        unemployedMean = sum(unemployedNum)/len(unemployedNum)
        return unemployedNum[idx] > unemployedMean

    #거래대금
    def getAmountLimitList(self, current, targetdf, amountdf, limit):
        beforebeforeOneMonth = current + pd.Timedelta(-1, unit='M') + pd.Timedelta(-1, unit='D')
        beforebeforeOneDay = current + pd.Timedelta(-2, unit='D')
        termTargetdf = targetdf.loc[beforebeforeOneMonth:beforebeforeOneDay]
        termAmountdf = amountdf.loc[beforebeforeOneMonth:beforebeforeOneDay]
        amount = (termTargetdf * termAmountdf).mean()
        return list(amount[amount>=limit].index)

    def getAmountPerMarcapLimitList(self, current, targetdf, amountdf, limit):
        beforebeforeOneMonth = current + pd.Timedelta(-1, unit='M') + pd.Timedelta(-1, unit='D')
        beforebeforeOneDay = current + pd.Timedelta(-2, unit='D')
        termTargetdf = targetdf.loc[beforebeforeOneMonth:beforebeforeOneDay]
        termAmountdf = amountdf.loc[beforebeforeOneMonth:beforebeforeOneDay]
        amount = (termTargetdf * termAmountdf).mean()
        return list(amount[amount>=limit].index)
    
    def getRaiseAmountList(self, current, targetdf, amountdf):
        beforebeforeOneMonth = current + pd.Timedelta(-1, unit='W') + pd.Timedelta(-1, unit='D')
        beforebeforeOneDay = current + pd.Timedelta(-2, unit='D')
        # termTargetdf = targetdf.loc[beforebeforeOneMonth:beforebeforeOneDay]
        termAmountdf = amountdf.loc[beforebeforeOneMonth:beforebeforeOneDay]
        amount = (amountdf.iloc[-1] - (termAmountdf).mean())
        return list(amount[amount>0].index)

    def getVPCI(self, current, targetdf, amountdf):
        #장기 50일 단기 10일
        #vwma 종가 * (거래량/산출기간의 총 거래량) + 종가 * (거래량/산출기간의 총 거래량) + ...
        #sma 이동평균
        #vpc= vwma(c,l) - sma(c,l)
        #vpr= vwma(c,a) - sma(c,s)
        #mv = sma(v,s) / sma (v,l)
        #vpci = vpc * vpr * vm

        longTerm = current + pd.Timedelta(-51, unit='D')
        shortTerm = current + pd.Timedelta(-11, unit='D')
        lately = current + pd.Timedelta(-1, unit='D')
        longAmount = amountdf.loc[longTerm:lately]
        shortAmount = amountdf.loc[shortTerm:lately]
        longEachAmount = longAmount/longAmount.sum()
        shortEachAmount = shortAmount/shortAmount.sum()

        longVWMA = (targetdf.loc[longTerm:lately] * longEachAmount).sum()
        longSMA = targetdf.loc[longTerm:lately].mean()
        shortVWMA = (targetdf.loc[shortTerm:lately] * shortEachAmount).sum()
        shortSMA = targetdf.loc[shortTerm:lately].mean()
        vpc = longVWMA - longSMA
        vpr = shortVWMA / shortSMA
        mv = shortAmount.mean()/longAmount.mean()
        vpci = vpc*vpr*mv

        return vpci

    def getVPCIShort(self, current, targetdf, amountdf):
        #장기 20일 단기 5일
        #vwma 종가 * (거래량/산출기간의 총 거래량) + 종가 * (거래량/산출기간의 총 거래량) + ...
        #sma 이동평균
        #vpc= vwma(c,l) - sma(c,l)
        #vpr= vwma(c,a) - sma(c,s)
        #mv = sma(v,s) / sma (v,l)
        #vpci = vpc * vpr * vm

        longTerm = current + pd.Timedelta(-21, unit='D')
        shortTerm = current + pd.Timedelta(-6, unit='D')
        lately = current + pd.Timedelta(-1, unit='D')
        longAmount = amountdf.loc[longTerm:lately]
        shortAmount = amountdf.loc[shortTerm:lately]
        longEachAmount = longAmount/longAmount.sum()
        shortEachAmount = shortAmount/shortAmount.sum()

        longVWMA = (targetdf.loc[longTerm:lately] * longEachAmount).sum()
        longSMA = targetdf.loc[longTerm:lately].mean()
        shortVWMA = (targetdf.loc[shortTerm:lately] * shortEachAmount).sum()
        shortSMA = targetdf.loc[shortTerm:lately].mean()
        vpc = longVWMA - longSMA
        vpr = shortVWMA / shortSMA
        mv = shortAmount.mean()/longAmount.mean()
        vpci = vpc*vpr*mv

        return vpci

    def getVPCILong(self, current, targetdf, amountdf):
        #장기 60일 단기 20일
        #vwma 종가 * (거래량/산출기간의 총 거래량) + 종가 * (거래량/산출기간의 총 거래량) + ...
        #sma 이동평균
        #vpc= vwma(c,l) - sma(c,l)
        #vpr= vwma(c,a) - sma(c,s)
        #mv = sma(v,s) / sma (v,l)
        #vpci = vpc * vpr * vm

        longTerm = current + pd.Timedelta(-61, unit='D')
        shortTerm = current + pd.Timedelta(-21, unit='D')
        lately = current + pd.Timedelta(-1, unit='D')
        longAmount = amountdf.loc[longTerm:lately]
        shortAmount = amountdf.loc[shortTerm:lately]
        longEachAmount = longAmount/longAmount.sum()
        shortEachAmount = shortAmount/shortAmount.sum()

        longVWMA = (targetdf.loc[longTerm:lately] * longEachAmount).sum()
        longSMA = targetdf.loc[longTerm:lately].mean()
        shortVWMA = (targetdf.loc[shortTerm:lately] * shortEachAmount).sum()
        shortSMA = targetdf.loc[shortTerm:lately].mean()
        vpc = longVWMA - longSMA
        vpr = shortVWMA / shortSMA
        mv = shortAmount.mean()/longAmount.mean()
        vpci = vpc*vpr*mv

        return vpci
    
    def getVPCIUpListLimit(self, current, targetdf, amountdf,limit):
        li = self.getVPCI(current, targetdf, amountdf)
        li = li.sort_values(ascending=False).head(limit)
        return list(li[li>0].index)

    def getVPCIUpList(self, current, targetdf, amountdf):
        li = self.getVPCI(current, targetdf, amountdf)
        return list(li[li>0].index)
    
    def getVPCIShortUpList(self, current, targetdf, amountdf):
        li = self.getVPCIShort(current, targetdf, amountdf)
        return list(li[li>0].index), li   

    def getVPCIShortDownList(self, current, targetdf, amountdf):
        li = self.getVPCIShort(current, targetdf, amountdf)
        return list(li[li<0].index), li

    def getVPCILongDownListBefore(self, current, targetdf, amountdf):
        li1 = self.getVPCILong(current + pd.Timedelta(-7, unit='D'), targetdf, amountdf)
        li2 = self.getVPCILong(current, targetdf, amountdf)
        li = li2 - li1
        res = list(set(li[li<0].index)&set(li2[li2<0].index))
        return res
    
    def getVPCIShortDownListBefore(self, current, targetdf, amountdf):
        li1 = self.getVPCIShort(current + pd.Timedelta(-7, unit='D'), targetdf, amountdf)
        li2 = self.getVPCIShort(current, targetdf, amountdf)
        li = li2 - li1
        res = list(set(li[li<0].index)&set(li2[li2<0].index))

        return res

    def getShortMomentumAmount(self, current, targetdf, amountdf):
        before = current + pd.Timedelta(-21, unit='D')
        shortTerm = current + pd.Timedelta(-6, unit='D')
        lately = current + pd.Timedelta(-1, unit='D')
        
        a =  targetdf.loc[shortTerm:lately].mean() - targetdf.loc[before:shortTerm].mean()
        b = (amountdf.iloc[amountdf.index.get_loc(lately, method='ffill')] - amountdf.iloc[amountdf.index.get_loc(shortTerm, method='ffill')])
        c = set(a[a<0].index)&set(b[b<0].index)
        return list(c)

    def getVPCIDownList(self, current, targetdf, amountdf):
        li = self.getVPCI(current, targetdf, amountdf)
        return list(li[li<0].index)
    
    def getVPCIUpListWeek(self, current, targetdf, amountdf,limit):
        li1 = self.getVPCI(current+ pd.Timedelta(-6, unit='D'), targetdf, amountdf)
        li2 = self.getVPCI(current+ pd.Timedelta(-5, unit='D'), targetdf, amountdf)
        li3 = self.getVPCI(current+ pd.Timedelta(-4, unit='D'), targetdf, amountdf)
        li4 = self.getVPCI(current+ pd.Timedelta(-3, unit='D'), targetdf, amountdf)
        li5 = self.getVPCI(current+ pd.Timedelta(-2, unit='D'), targetdf, amountdf)
        li6 = self.getVPCI(current+ pd.Timedelta(-1, unit='D'), targetdf, amountdf)
        li7 = self.getVPCI(current, targetdf, amountdf)
        t1 = li7-li6
        t2 = li6-li5
        t3 = li5-li4
        t4 = li4-li3
        t5 = li3-li2
        t6 = li2-li1
        t1 = t1[t1>=0].index.to_numpy()
        t2 = t2[t2>=0].index.to_numpy()
        t3 = t3[t3>=0].index.to_numpy()
        t4 = t4[t4>=0].index.to_numpy()
        t5 = t5[t5>=0].index.to_numpy()
        t6 = t6[t6>=0].index.to_numpy()
        arr = np.concatenate((t1, t2, t3, t4, t5, t6))
        print('up',arr)
        unique, counts= np.unique(arr, return_counts=True)
        return list(unique[np.where(counts>=5)[0]])
    
    def getVPCIDownListWeek(self, current, targetdf, amountdf):
        li1 = self.getVPCI(current + pd.Timedelta(-6, unit='D'), targetdf, amountdf)
        li2 = self.getVPCI(current + pd.Timedelta(-5, unit='D'), targetdf, amountdf)
        li3 = self.getVPCI(current + pd.Timedelta(-4, unit='D'), targetdf, amountdf)
        li4 = self.getVPCI(current + pd.Timedelta(-3, unit='D'), targetdf, amountdf)
        li5 = self.getVPCI(current + pd.Timedelta(-2, unit='D'), targetdf, amountdf)
        li6 = self.getVPCI(current + pd.Timedelta(-1, unit='D'), targetdf, amountdf)
        li7 = self.getVPCI(current, targetdf, amountdf)
        t1 = li7-li6
        t2 = li6-li5
        t3 = li5-li4
        t4 = li4-li3
        t5 = li3-li2
        t6 = li2-li1
        t1 = t1[t1<0].index.to_numpy()
        t2 = t2[t2<0].index.to_numpy()
        t3 = t3[t3<0].index.to_numpy()
        t4 = t4[t4<0].index.to_numpy()
        t5 = t5[t5<0].index.to_numpy()
        t6 = t6[t6<0].index.to_numpy()
        arr = np.concatenate((t1, t2, t3, t4, t5, t6))
        print('down',arr)
        unique, counts = np.unique(arr, return_counts=True)
        return list(unique[np.where(counts>=2)[0]])

    
    def getUpList(self, current, targetdf, amountdf):
        before2 = current + pd.Timedelta(-51, unit='D')
        before = current + pd.Timedelta(-11, unit='D')
        lately = current + pd.Timedelta(-1)
        t = targetdf.loc[before:lately].mean() - targetdf.loc[before2:lately].mean()

        li2 = self.getVPCI(before, targetdf, amountdf)
        li = self.getVPCI(current, targetdf, amountdf)
        t2 = li - li2
        print(t, t2)
        return list(set(t[t>0].index)&set(t2[t2>0].index))
    
    def getUpListLimit(self, current, targetdf, amountdf,limit):
        before2 = current + pd.Timedelta(-51, unit='D')
        before = current + pd.Timedelta(-11, unit='D')
        lately = current + pd.Timedelta(-1)
        t = targetdf.loc[before:lately].mean() - targetdf.loc[before2:lately].mean()

        li2 = self.getVPCI(before, targetdf, amountdf)
        li = self.getVPCI(current, targetdf, amountdf)
        t2 = li - li2
        t2 = t2.sort_values(ascending=False).head(limit)
        print(t2)
        return list(set(t[t>0].index)&set(t2[t2>0].index))
    
    

    
    def getAmount(self, current, targetdf, target, sName, sCode, limit):
        targetCode = list(map(lambda x : sName[x], target))
        targetdf = targetdf[targetdf['Code'].isin(targetCode)]
        beforebeforeOneMonth = current + pd.Timedelta(-1, unit='M') + pd.Timedelta(-1, unit='D')
        beforebeforeOneDay = current + pd.Timedelta(-2, unit='D')
        termdf = targetdf.loc[beforebeforeOneMonth:beforebeforeOneDay]
        codes = termdf['Code'].values
        results = []
        groupMean = termdf['Amount'].groupby(termdf['Code']).mean()
        for code in codes:
            if groupMean[code] > limit:
                if code in sCode.keys():
                    results.append(sCode[code])
        return list(set(results))


    def getRsi30perList(self, current, targetdf, limit, minVal=0):
        raiseDf = targetdf - targetdf.shift(1)
        beforebeforeOneMonth = current + pd.Timedelta(-1, unit='M') + pd.Timedelta(-1, unit='D')
        beforebeforeOneDay = current + pd.Timedelta(-2, unit='D')
        
        raiseDf = raiseDf[beforebeforeOneMonth:beforebeforeOneDay]
        AU = raiseDf[raiseDf > 0].mean()
        AD = raiseDf[raiseDf < 0].applymap(lambda val: abs(val)).mean()
        beforeRsi = AU / (AU + AD) * 100

        beforeOneMonth = current + pd.Timedelta(-1, unit='M')
        beforeOneDay = current + pd.Timedelta(-1, unit='D')
        
        raiseDf = raiseDf[beforeOneMonth:beforeOneDay]
        AU = raiseDf[raiseDf > 0].mean()
        AD = raiseDf[raiseDf < 0].applymap(lambda val: abs(val)).mean()
        rsi = AU / (AU + AD) * 100

        beforeIndex = list(beforeRsi[beforeRsi < 30].sort_values(ascending=True).index)
        curIndex = list(rsi[rsi >= 30].sort_values(ascending=False).index)

        return  list(set(beforeIndex) & set(curIndex))[0:limit]
         
    def getVarienceList(self, current, targetdf, limit, ascending):
        beforeOneMonth = current + pd.Timedelta(-12, unit='M')
        beforeOneDay = current + pd.Timedelta(-1, unit='D')
        datadf = targetdf[beforeOneMonth:beforeOneDay]
        raiseDf = (datadf - datadf.shift(1)).applymap(lambda val: abs(val))
        variencedf = datadf.mean()/raiseDf.mean()
        print('varience',variencedf)
        return list(variencedf.sort_values(ascending=ascending).head(limit).index)


    def getRiseMeanList(self, current, targetdf, limit, minVal=0):
        raiseDf = targetdf - targetdf.shift(1)
        beforeOneMonth = current + pd.Timedelta(-1, unit='M')
        beforeOneDay = current + pd.Timedelta(-1, unit='D')
        raiseDf = raiseDf[beforeOneMonth:beforeOneDay]

        raiseIndexDict = {}
        declineIndexDict = {}
        for col in raiseDf.columns:
            raiseIndexDict[col] = (raiseDf[col][raiseDf[col]>0].index)
            declineIndexDict[col] = (raiseDf[col][raiseDf[col]<0].index)
        unitTargetdf = targetdf[beforeOneMonth:beforeOneDay]
        resultDict = {}
        for col in unitTargetdf.columns:
            resultDict[col] = unitTargetdf[col].loc[raiseIndexDict[col]].mean() - unitTargetdf[col].loc[declineIndexDict[col]].mean()
        # mdf = unitTargetdf.loc[raiseIndexDict[col]].mean() - unitTargetdf.loc[declineIndexDict[col]].mean()
        # mdf = mdf[mdf > 0]
        mdf = pd.Series(resultDict)
        mdf = mdf[mdf>0]
        # mdf = raiseDf[beforeOneMonth:beforeOneDay].mean(axis=0)/targetdf[beforeOneMonth:beforeOneDay].mean(axis=0)
        # mdfRaise = mdf[mdf > minVal]
        # mdfLow = mdf[]
        return list(mdf.sort_values(ascending=False).head(limit).index)
        

    def getFactorList(self, current, targetdf, factordf, factor, sName, sCode, ascending, num, minVal=float('-inf'), maxVal=float('inf')):
        # yearDf = factordf[factor][factordf[factor]['종목명'].isin(list(targetdf.columns))]
        codeList = list(map(lambda x: sName[x], list(targetdf.columns)))
        yearDf = factordf[factor][factordf[factor].index.isin(codeList)]
        
        # yearDf = factordf[factor][factordf[factor].index.isin(targetdf.columns)]
        # print(factordf[factor])
        if current.month > 4:
            yearDf = yearDf[current.year - 1]
        else:
            yearDf = yearDf[current.year - 2]
        yearDf = yearDf.dropna()
        yearDf = yearDf[yearDf >= minVal]
        yearDf = yearDf[yearDf <= maxVal]
        # intersect = list(set(yearDf.columns) & set(nameList))
        shcodes = list(map(lambda x : sCode[x], list(yearDf.sort_values(ascending=ascending).head(num).index)))
        # nameList = list(factordf[factor].loc[shcodes].index)
        intersect = list(set(targetdf.columns) & set(shcodes))
        return intersect
    
    
    
    def getFactorListComp(self, current, targetdf, factordf, factor, sName, sCode, ascending, num, minVal=float('-inf'), maxVal=float('inf')):
        # yearDf = factordf[factor][factordf[factor]['종목명'].isin(list(targetdf.columns))]
        codeList = list(map(lambda x: sName[x], list(targetdf.columns)))
        yearDf = factordf[factor][factordf[factor].index.isin(codeList)]
        beforeDf = yearDf
        if current.month > 4:
            yearDf = yearDf[current.year - 1]
            beforeDf = (beforeDf[current.year - 2] if current.year > 2008 else pd.DataFrame())
        else:
            yearDf = yearDf[current.year - 2]
            beforeDf = (beforeDf[current.year - 3] if current.year > 2009 else pd.DataFrame())
        if not beforeDf.empty:
            yearDf = yearDf.dropna()
            beforeDf = beforeDf.dropna()
            inter = list(set(yearDf.index)&set(beforeDf.index))
            comp = yearDf[inter] - beforeDf[inter]
            comp = comp[comp >= minVal]
            comp = comp[comp <= maxVal]
            shcodes = list(map(lambda x : sCode[x], list(comp.sort_values(ascending=ascending).head(num).index)))
            intersect = list(set(targetdf.columns) & set(shcodes))
            return intersect
        else:
            yearDf = yearDf.dropna()
            yearDf = yearDf[yearDf >= minVal]
            yearDf = yearDf[yearDf <= maxVal]
            # intersect = list(set(yearDf.columns) & set(nameList))
            shcodes = list(map(lambda x : sCode[x], list(yearDf.sort_values(ascending=ascending).head(num).index)))
            # nameList = list(factordf[factor].loc[shcodes].index)
            intersect = list(set(targetdf.columns) & set(shcodes))
            return intersect
    
    def getQuarterFactorList(self, current, targetdf, factordf, factor, sName, sCode, ascending, num, minVal=float('-inf'), maxVal=float('inf')):
        # yearDf = factordf[factor][factordf[factor]['종목명'].isin(list(targetdf.columns))]
        codeList = list(map(lambda x: sName[x], list(targetdf.columns)))
        yearDf = factordf[factor][factordf[factor].index.isin(codeList)]
        
        # yearDf = factordf[factor][factordf[factor].index.isin(targetdf.columns)]
        # print(factordf[factor])
        print(current, current.month)
        if current.month > 9:
            #반기보고서
            yearDf = yearDf[float(str(current.year)+'.06')]
        elif current.month > 5:
            #1/4분기보고서
            yearDf = yearDf[float(str(current.year)+'.03')]
        elif current.month > 4:
            #사업보고서
            yearDf = yearDf[float(str(current.year-1)+'.12')]
        elif current.month > 0:
            #3/4분기보고서
            yearDf = yearDf[float(str(current.year-1)+'.09')]

        yearDf = yearDf.dropna()
        yearDf = yearDf[yearDf >= minVal]
        yearDf = yearDf[yearDf <= maxVal]
        # intersect = list(set(yearDf.columns) & set(nameList))
        shcodes = list(map(lambda x : sCode[x], list(yearDf.sort_values(ascending=ascending).head(num).index)))
        # nameList = list(factordf[factor].loc[shcodes].index)
        intersect = list(set(targetdf.columns) & set(shcodes))
        return intersect

    def getFactorPerStockNum(self, current, targetdf, factordf, factor, marcapdf, sCode, sName, ascending, num, minVal=float('-inf'), maxVal=float('inf') ):
        # yearDf = factordf[factor][factordf[factor]['종목명'].isin(list(targetdf.columns))]
        # yearDf = factordf[factor][factordf[factor].index.isin(targetdf.columns)]
        codeList = list(map(lambda x: sName[x], targetdf.columns))
        yearDf = factordf[factor][factordf[factor].index.isin(codeList)]
        # print(factordf[factor])
        if current.month > 4:
            yearDf = yearDf[current.year - 1]
        else:
            yearDf = yearDf[current.year - 2]
        yearDf = yearDf.dropna()
        # intersect = list(set(yearDf.columns) & set(nameList))
        
        if marcapdf.index[-1] > current:
            marcapdf2 = marcapdf[str(current.year)+'-'+str(current.month)]
        else:
            lastDate = marcapdf.index[-1]
            marcapdf2 = marcapdf[str(lastDate.year)+'-'+str(lastDate.month)]
        inter = list(set(sCode.keys()) & set(marcapdf2['Code'].values))
        marcapdf2 = marcapdf2[marcapdf2['Code'].isin(inter)]

        # indexes = list(map(lambda x : sCode[x], marcapdf2['Code'].values))
        indexes = list(marcapdf2['Code'].values)
        values = list(marcapdf2['Stocks'].values)
        print(len(indexes), len(values))
        stockNum = pd.Series(values, index=indexes)
        inter = list(set(yearDf.index) & set(indexes))
        deleteList = []
        for index in list(yearDf.index):
            if index not in stockNum.index:
                deleteList.append(index)
                continue
            if type(stockNum.at[index]) is np.float64:
                # print(stockNum)
                yearDf.at[index] = yearDf.at[index] / stockNum.at[index]
            if type(stockNum.at[index]) is np.array:
                yearDf.at[index] = yearDf.at[index] / stockNum.at[index][0]
        # if len(deleteList) > 0:
        #     print(deleteList, sCode[deleteList[0]])
        yearDf = yearDf.drop(deleteList, axis=0)
        yearDf = yearDf[yearDf >= minVal]
        yearDf = yearDf[yearDf <= maxVal]
        shcodes = list(map(lambda x : sCode[x], list(yearDf.sort_values(ascending=ascending).head(num).index)))
        # nameList = list(factordf[factor].loc[shcodes].index)
        intersect = list(set(targetdf.columns) & set(shcodes))
        return intersect

    def getQuarterFactorPerStockNum(self, current, targetdf, factordf, factor, marcapdf, sCode, sName, ascending, num, minVal=float('-inf'), maxVal=float('inf') ):
        # yearDf = factordf[factor][factordf[factor]['종목명'].isin(list(targetdf.columns))]
        # yearDf = factordf[factor][factordf[factor].index.isin(targetdf.columns)]
        codeList = list(map(lambda x: sName[x], targetdf.columns))
        yearDf = factordf[factor][factordf[factor].index.isin(codeList)]
        # print(factordf[factor])
        if current.month > 9:
            #반기보고서
            yearDf = yearDf[float(str(current.year)+'.06')]
        elif current.month > 5:
            #1/4분기보고서
            yearDf = yearDf[float(str(current.year)+'.03')]
        elif current.month > 4:
            #사업보고서
            yearDf = yearDf[float(str(current.year-1)+'.12')]
        elif current.month > 0:
            #3/4분기보고서
            yearDf = yearDf[float(str(current.year-1)+'.09')]

        yearDf = yearDf.dropna()
        # intersect = list(set(yearDf.columns) & set(nameList))
        
        if marcapdf.index[-1] > current:
            marcapdf2 = marcapdf[str(current.year)+'-'+str(current.month)]
        else:
            lastDate = marcapdf.index[-1]
            marcapdf2 = marcapdf[str(lastDate.year)+'-'+str(lastDate.month)]
        inter = list(set(sCode.keys()) & set(marcapdf2['Code'].values))
        marcapdf2 = marcapdf2[marcapdf2['Code'].isin(inter)]

        # indexes = list(map(lambda x : sCode[x], marcapdf2['Code'].values))
        indexes = list(marcapdf2['Code'].values)
        values = list(marcapdf2['Stocks'].values)
        print(len(indexes), len(values))
        stockNum = pd.Series(values, index=indexes)
        inter = list(set(yearDf.index) & set(indexes))
        deleteList = []
        for index in list(yearDf.index):
            if index not in stockNum.index:
                deleteList.append(index)
                continue
            if type(stockNum.at[index]) is np.float64:
                # print(stockNum)
                yearDf.at[index] = yearDf.at[index] / stockNum.at[index]
            if type(stockNum.at[index]) is np.array:
                yearDf.at[index] = yearDf.at[index] / stockNum.at[index][0]
        # if len(deleteList) > 0:
        #     print(deleteList, sCode[deleteList[0]])
        yearDf = yearDf.drop(deleteList, axis=0)
        yearDf = yearDf[yearDf >= minVal]
        yearDf = yearDf[yearDf <= maxVal]
        shcodes = list(map(lambda x : sCode[x], list(yearDf.sort_values(ascending=ascending).head(num).index)))
        # nameList = list(factordf[factor].loc[shcodes].index)
        intersect = list(set(targetdf.columns) & set(shcodes))
        return intersect
    
    def getCurValuePerStockNumFactor(self, current, targetdf, factordf, factor, marcapdf, sCode, sName, factorUnit, ascending, num, minVal=float('-inf'), maxVal=float('inf') ):
        # yearDf = factordf[factor][factordf[factor]['종목명'].isin(list(targetdf.columns))]
        # yearDf = factordf[factor][factordf[factor].index.isin(targetdf.columns)]
        codeList = list(map(lambda x: sName[x], targetdf.columns))
        yearDf = factordf[factor][factordf[factor].index.isin(codeList)]
        # print(factordf[factor])
        if current.month > 4:
            yearDf = yearDf[current.year - 1]
        else:
            yearDf = yearDf[current.year - 2]
        yearDf = yearDf.dropna()
        # intersect = list(set(yearDf.columns) & set(nameList))
        
        if marcapdf.index[0] > current:
            initDate = marcapdf.index[0]
            marcapdf2 = marcapdf[str(initDate.year)+'-'+str(initDate.month)]
        elif marcapdf.index[-1] > current:
            marcapdf2 = marcapdf[str(current.year)+'-'+str(current.month)]
        else:
            lastDate = marcapdf.index[-1]
            marcapdf2 = marcapdf[str(lastDate.year)+'-'+str(lastDate.month)]
        inter = list(set(sCode.keys()) & set(marcapdf2['Code'].values))
        marcapdf2 = marcapdf2[marcapdf2['Code'].isin(inter)]

        # indexes = list(map(lambda x : sCode[x], marcapdf2['Code'].values))
        indexes = list(marcapdf2['Code'].values)
        values = list(marcapdf2['Stocks'].values)
        print(len(indexes), len(values))
        stockNum = pd.Series(values, index=indexes)
        inter = list(set(yearDf.index) & set(indexes))
        deleteList = []
        before = current + pd.Timedelta(-1, 'D')
        for index in list(yearDf.index):
            if index not in stockNum.index:
                deleteList.append(index)
                continue
            value = targetdf.iloc[targetdf.index.get_loc(before, method='ffill')][sCode[index]]
            if type(stockNum.at[index]) is np.float64:
                yearDf.at[index] = value * stockNum.at[index] / (yearDf.at[index] * factorUnit) 
            else:
                yearDf.at[index] = value * stockNum.at[index][0] / (yearDf.at[index] * factorUnit)
        # if len(deleteList) > 0:
        yearDf = yearDf.drop(deleteList, axis=0)
        yearDf = yearDf[yearDf >= minVal]
        yearDf = yearDf[yearDf <= maxVal]
        shcodes = list(map(lambda x : sCode[x], list(yearDf.sort_values(ascending=ascending).head(num).index)))
        # nameList = list(factordf[factor].loc[shcodes].index)
        intersect = list(set(targetdf.columns) & set(shcodes))
        return intersect

    def getMinusFactorPerCap(self, current, targetdf, factordf, factor, factor2, marcapdf, sCode, sName, factorUnit, ascending, num, minVal=float('-inf'), maxVal=float('inf') ):
        # yearDf = factordf[factor][factordf[factor]['종목명'].isin(list(targetdf.columns))]
        # yearDf = factordf[factor][factordf[factor].index.isin(targetdf.columns)]
        codeList = list(map(lambda x: sName[x], targetdf.columns))
        yearDf = factordf[factor][factordf[factor].index.isin(codeList)]
        yearDf2 = factordf[factor2][factordf[factor2].index.isin(codeList)]
        # print(factordf[factor])
        if current.month > 4:
            yearDf = yearDf[current.year - 1]
            yearDf2 = yearDf2[current.year - 1]
        else:
            yearDf = yearDf[current.year - 2]
            yearDf2 = yearDf2[current.year - 2]
        yearDf = yearDf.dropna()
        yearDf2 = yearDf2.dropna()
        # intersect = list(set(yearDf.columns) & set(nameList))
        
        if marcapdf.index[0] > current:
            initDate = marcapdf.index[0]
            marcapdf2 = marcapdf[str(initDate.year)+'-'+str(initDate.month)]
        elif marcapdf.index[-1] > current:
            marcapdf2 = marcapdf[str(current.year)+'-'+str(current.month)]
        else:
            lastDate = marcapdf.index[-1]
            marcapdf2 = marcapdf[str(lastDate.year)+'-'+str(lastDate.month)]
        inter = list(set(sCode.keys()) & set(marcapdf2['Code'].values))
        marcapdf2 = marcapdf2[marcapdf2['Code'].isin(inter)]

        # indexes = list(map(lambda x : sCode[x], marcapdf2['Code'].values))
        indexes = list(marcapdf2['Code'].values)
        values = list(marcapdf2['Stocks'].values)
        print(len(indexes), len(values))
        stockNum = pd.Series(values, index=indexes)
        print()
        inter = list(set(yearDf.index) & set(indexes) & set(yearDf2.index))
        deleteList = []
        before = current + pd.Timedelta(-1, 'D')
        for index in yearDf.index:
            if index not in stockNum.index or index not in yearDf2.index:
                deleteList.append(index)
                continue

            value = targetdf.iloc[targetdf.index.get_loc(before, method='ffill')][sCode[index]]
            if type(stockNum.at[index]) is np.float64:
                yearDf.at[index] = ((yearDf.at[index] - yearDf2[index]) * factorUnit) / value * stockNum.at[index]
            else:
                yearDf.at[index] = ((yearDf.at[index] - yearDf2[index]) * factorUnit) / value * stockNum.at[index][0]
        # if len(deleteList) > 0:
        yearDf = yearDf.drop(deleteList, axis=0)
        yearDf = yearDf[yearDf >= minVal]
        yearDf = yearDf[yearDf <= maxVal]
        shcodes = list(map(lambda x : sCode[x], list(yearDf.sort_values(ascending=ascending).head(num).index)))
        # nameList = list(factordf[factor].loc[shcodes].index)
        intersect = list(set(targetdf.columns) & set(shcodes))
        return intersect

    
    def getQuarterCurValuePerStockNumFactor(self, current, targetdf, factordf, factor, marcapdf, sCode, sName, factorUnit, ascending, num, minVal=float('-inf'), maxVal=float('inf') ):
        # yearDf = factordf[factor][factordf[factor]['종목명'].isin(list(targetdf.columns))]
        # yearDf = factordf[factor][factordf[factor].index.isin(targetdf.columns)]
        codeList = list(map(lambda x: sName[x], targetdf.columns))
        yearDf = factordf[factor][factordf[factor].index.isin(codeList)]
        # print(factordf[factor])
        if current.month > 9:
            #반기보고서
            yearDf = yearDf[float(str(current.year)+'.06')]
        elif current.month > 5:
            #1/4분기보고서
            yearDf = yearDf[float(str(current.year)+'.03')]
        elif current.month > 4:
            #사업보고서
            yearDf = yearDf[float(str(current.year-1)+'.12')]
        elif current.month > 0:
            #3/4분기보고서
            yearDf = yearDf[float(str(current.year-1)+'.09')]

        yearDf = yearDf.dropna()
        # intersect = list(set(yearDf.columns) & set(nameList))
        
        if marcapdf.index[0] > current:
            initDate = marcapdf.index[0]
            marcapdf2 = marcapdf[str(initDate.year)+'-'+str(initDate.month)]
        elif marcapdf.index[-1] > current:
            marcapdf2 = marcapdf[str(current.year)+'-'+str(current.month)]
        else:
            lastDate = marcapdf.index[-1]
            marcapdf2 = marcapdf[str(lastDate.year)+'-'+str(lastDate.month)]
        inter = list(set(sCode.keys()) & set(marcapdf2['Code'].values))
        marcapdf2 = marcapdf2[marcapdf2['Code'].isin(inter)]

        # indexes = list(map(lambda x : sCode[x], marcapdf2['Code'].values))
        indexes = list(marcapdf2['Code'].values)
        values = list(marcapdf2['Stocks'].values)
        print(len(indexes), len(values))
        stockNum = pd.Series(values, index=indexes)
        inter = list(set(yearDf.index) & set(indexes))
        deleteList = []
        for index in list(yearDf.index):
            if index not in stockNum.index:
                deleteList.append(index)
                continue
            value = targetdf.iloc[targetdf.index.get_loc(current, method='ffill')][sCode[index]]
            if type(stockNum.at[index]) is np.float64:
                yearDf.at[index] = value * stockNum.at[index] / (yearDf.at[index] * factorUnit) 
            else:
                yearDf.at[index] = value * stockNum.at[index][0] / (yearDf.at[index] * factorUnit)
        # if len(deleteList) > 0:
        yearDf = yearDf.drop(deleteList, axis=0)
        yearDf = yearDf[yearDf >= minVal]
        yearDf = yearDf[yearDf <= maxVal]
        shcodes = list(map(lambda x : sCode[x], list(yearDf.sort_values(ascending=ascending).head(num).index)))
        # nameList = list(factordf[factor].loc[shcodes].index)
        intersect = list(set(targetdf.columns) & set(shcodes))
        return intersect
    
    def getFactorRank(self, current, targetdf, factordf, factor, sName, sCode):
        codeList = list(map(lambda x: sName[x], targetdf.columns))
        yearDf = factordf[factor][factordf[factor].index.isin(codeList)]
        # yearDf = factordf[factor][factordf[factor].isin(targetdf.columns)]
        if current.month > 4:
            yearDf = yearDf[current.year - 1]
        else:
            yearDf = yearDf[current.year - 2]
        sortedDf = yearDf.sort_values(ascending=True)
        # print(sortedDf)
        # return sortedDf
        # return pd.Series(range(0,len(sortedDf.index)),index=sortedDf.index)
        return pd.Series(sortedDf.values,index=map(lambda x: sCode[x], sortedDf.index))
    
    def getFactorRankSort(self, current, targetdf, factordf, factor, sName, sCode, ascending):
        codeList = list(map(lambda x: sName[x], targetdf.columns))
        yearDf = factordf[factor][factordf[factor].index.isin(codeList)]
        # yearDf = factordf[factor][factordf[factor].isin(targetdf.columns)]
        if current.month > 4:
            yearDf = yearDf[current.year - 1]
        else:
            yearDf = yearDf[current.year - 2]
        sortedDf = yearDf.sort_values(ascending=ascending)
        # print(sortedDf)
        # return sortedDf
        # return pd.Series(range(0,len(sortedDf.index)),index=sortedDf.index)
        return pd.Series(sortedDf.values,index=map(lambda x: sCode[x], sortedDf.index))
    
    def getFactorLists(self, current, targetdf, factordf, factors, num, weights, sName, sCode):
        rankList = []
        curIndex = targetdf.index.get_loc(current, method='ffill')
        targetIdx = targetdf.iloc[curIndex].dropna().index
        datadf = pd.DataFrame(index=targetIdx)
        for factor in factors:
            datadf[factor] = self.getFactorRank(current, targetdf, factordf, factor, sName, sCode)
        datadf['sum'] = 0
        for factor in factors:
            datadf['sum'] += weights[factor] * datadf[factor]
        return list(datadf['sum'].dropna().sort_values(ascending=False).head(num).index)

    def getFactorListsStd(self, current, targetdf, factordf, factors, num, weights, sName, sCode):
        rankList = []
        curIndex = targetdf.index.get_loc(current, method='ffill')
        targetIdx = targetdf.iloc[curIndex].dropna().index
        datadf = pd.DataFrame(index=targetIdx)
        for factor in factors:
            datadf[factor] = self.getFactorRank(current, targetdf, factordf, factor, sName, sCode)
        datadf['sum'] = 0
        mean = datadf[factor].mean()
        std = datadf[factor].std()
        for factor in factors:
            datadf['sum'] += weights[factor] * (datadf[factor] - mean / std)
        return list(datadf['sum'].dropna().sort_values(ascending=False).head(num).index)

    def getFactorListsRank(self, current, targetdf, factordf, factors, sName, sCode, ascending, num, minVal=float('-inf'), maxVal=float('inf')):
        # yearDf = factordf[factor][factordf[factor]['종목명'].isin(list(targetdf.columns))]
        curIndex = targetdf.index.get_loc(current, method='ffill')
        targetIdx = targetdf.iloc[curIndex].dropna().index
        datadf = pd.DataFrame(index=targetIdx)
        for idx in range(len(factors)):
            datadf[factor] = self.getFactorRankSort(current, targetdf, factordf, factors[i], sName, sCode,ascending[i])
        datadf['sum'] = 0
        for factor in factors:
            datadf['sum'] += datadf[factor]
        return list(datadf['sum'].dropna().sort_values(ascending=True).head(num).index)

    def getMoneyRate(self, nameList):
        rate = pd.Series([1/len(nameList)]*len(nameList), index=nameList)
        return rate
