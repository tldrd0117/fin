import pandas as pd

class StockStrategy:
    @staticmethod
    def create():
        stockStrategy = StockStrategy()
        return stockStrategy
    
    def getMomentumList(self, current, targetdf, mNum, mUnit, limit, minVal=-1):
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
        # momentumScore = momentumScore.query('')
        # momentumScore = momentumScore[momentumScore >= minVal]
        
        return list(momentumScore.mean().sort_values(ascending=False).head(limit).index)
    
    def getRiseMeanList(self, current, targetdf, limit, minVal=0):
        raiseDf = targetdf - targetdf.shift(1)
        beforeOneMonth = current + pd.Timedelta(-1, unit='M')
        beforeOneDay = current + pd.Timedelta(-1, unit='D')
        mdf = raiseDf[beforeOneMonth:beforeOneDay].mean(axis=0)/targetdf[beforeOneMonth:beforeOneDay].mean(axis=0)
        mdf = mdf[mdf > minVal]
        return list(mdf.sort_values(ascending=False).head(limit).index)
        

    def getFactorList(self, current, targetdf, factordf, factor, ascending, num, minVal=-10000000, maxVal=10000000):
        yearDf = factordf[factor][factordf[factor]['종목명'].isin(list(targetdf.columns))]
        if current.month > 4:
            yearDf = yearDf[current.year - 1]
        else:
            yearDf = yearDf[current.year - 2]
        yearDf = yearDf[yearDf >= minVal]
        yearDf = yearDf[yearDf <= maxVal]
        # intersect = list(set(yearDf.columns) & set(nameList))
        shcodes = list(yearDf.sort_values(ascending=ascending).head(num).index)
        nameList = list(factordf[factor].loc[shcodes]['종목명'])
        intersect = list(set(targetdf.columns) & set(nameList))
        return intersect

    def getMoneyRate(self, nameList):
        rate = pd.Series([1/len(nameList)]*len(nameList), index=nameList)
        return rate
