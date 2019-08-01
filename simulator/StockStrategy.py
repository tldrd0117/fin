import pandas as pd
import math

class StockStrategy:
    @staticmethod
    def create():
        stockStrategy = StockStrategy()
        return stockStrategy
    
    def getCurrentFactor(self, current, factordf, targetdf, factor):
        yearDf = factordf[factor][factordf[factor].index.isin(targetdf.columns)]
        # print(factordf[factor])
        if current.month > 4:
            yearDf = yearDf[current.year - 1]
        else:
            yearDf = yearDf[current.year - 2]
        return yearDf

    def getFactor(self, current, factordf, factor, code):
        df = factordf[factor][factordf[factor].index.isin([code])]
        if len(df.index) <= 0:
            return 'None'
        if current.month > 4:
            return df.loc[code, current.year - 1]
        else:
            return df.loc[code, current.year - 2]

    def filterAltmanZScore(self, current, targetdf, factordf, topcap ):
        floatingAsset = self.getCurrentFactor(current, factordf, targetdf, '유동자산')
        floatingLiablilities = self.getCurrentFactor(current, factordf, targetdf, '유동부채')
        totalAsset = self.getCurrentFactor(current, factordf, targetdf, '자산')
        liablilities = self.getCurrentFactor(current, factordf, targetdf, '자산')
        retainedEarning = self.getCurrentFactor(current, factordf, targetdf, '이익잉여금')
        sales = self.getCurrentFactor(current, factordf, targetdf, '매출액')
        ebit = self.getCurrentFactor(current, factordf, targetdf, 'ebit')
        topcap = topcap.resample('A').first()
        loc = topcap.index.get_loc(current, method='pad')
        marketValueOfEquity = topcap.iloc[loc]['Marcap']
        
        x1 = (floatingAsset - floatingLiablilities) / totalAsset
        x2 = retainedEarning / totalAsset
        x3 = ebit / totalAsset
        x4 = marketValueOfEquity / liablilities
        x5 = sales / totalAsset
        altmanZ = 1.2 * x1 + 1.4 * x2 + 3.3 * x3 + 0.6 * x4 + 0.999 * x5
        return list(altmanZ[altmanZ >= 1.81].index)
    
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
        start = mdf.index.get_loc(beforeMomentumDate, method='nearest')
        end = mdf.index.get_loc(current, method='nearest')
        
        oneYearDf = mdf.iloc[start:end+1]
        latelyValue = oneYearDf.iloc[-1]
        momentum = pd.DataFrame(latelyValue.values - oneYearDf.values, oneYearDf.index, oneYearDf.columns)
        # momentumScore = momentum.applymap(lambda val: 1 if val > 0 else 0 )
        momentumValues = momentum.values
        momentumValues[momentumValues>0] = 1
        momentumValues[momentumValues<=0] = -1
        momentumScore = pd.DataFrame(momentumValues, momentum.index, momentum.columns)
        return momentumScore.mean(axis=0)
    
    def getMomentumList(self, current, targetdf, mNum, mUnit, limit, minVal=-100, maxVal=100):
        momentumScore = self.getMinusMomentumScore(current, targetdf, mNum, mUnit)
        # momentumScore = momentumScore.query('')
        momentumScore = momentumScore[momentumScore >= minVal]
        momentumScore = momentumScore[momentumScore <= maxVal]
        sixMonthMomentumScore = self.getMinusMomentumScore(current, targetdf, 6, 'M')
        
        return list(momentumScore.sort_values(ascending=False).head(limit).index), sixMonthMomentumScore.sum()
    
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
        

    def getFactorList(self, current, targetdf, factordf, factor, ascending, num, minVal=float('-inf'), maxVal=float('inf')):
        # yearDf = factordf[factor][factordf[factor]['종목명'].isin(list(targetdf.columns))]
        yearDf = factordf[factor][factordf[factor].index.isin(targetdf.columns)]
        # print(factordf[factor])
        if current.month > 4:
            yearDf = yearDf[current.year - 1]
        else:
            yearDf = yearDf[current.year - 2]
        yearDf = yearDf.dropna()
        yearDf = yearDf[yearDf >= minVal]
        yearDf = yearDf[yearDf <= maxVal]
        # intersect = list(set(yearDf.columns) & set(nameList))
        shcodes = list(yearDf.sort_values(ascending=ascending).head(num).index)
        # nameList = list(factordf[factor].loc[shcodes].index)
        intersect = list(set(targetdf.columns) & set(shcodes))
        return intersect
    
    def getFactorRank(self, current, targetdf, factordf, factor):
        yearDf = factordf[factor][factordf[factor].isin(targetdf.columns)]
        if current.month > 4:
            yearDf = yearDf[current.year - 1]
        else:
            yearDf = yearDf[current.year - 2]
        sortedDf = yearDf.sort_values(ascending=True)
        return pd.Series(range(0,len(sortedDf.index)),index=sortedDf.index)
    
    def getFactorLists(self, current, targetdf, factordf, factors, num, weights):
        rankList = []
        curIndex = targetdf.index.get_loc(current, method='nearest')
        targetIdx = targetdf.iloc[curIndex].dropna().index
        datadf = pd.DataFrame(index=targetIdx)
        for factor in factors:
            datadf[factor] = self.getFactorRank(current, targetdf, factordf, factor)
        datadf['sum'] = 0
        for factor in factors:
            datadf['sum'] += weights[factor] * datadf[factor]
        return list(datadf['sum'].dropna().sort_values(ascending=False).head(num).index)

    def getMoneyRate(self, nameList):
        rate = pd.Series([1/len(nameList)]*len(nameList), index=nameList)
        return rate
