import pandas as pd

class StockTransaction:
    @staticmethod
    def create():
        st = StockTransaction()
        return st
    
    def getMomentumMean(self, current, df, mNum, mUnit):
        mdf = df.resample('M').mean()
        beforeMomentumDate = current + pd.Timedelta(-mNum, unit=mUnit)
        start = mdf.index.get_loc(beforeMomentumDate, method='nearest')
        end = mdf.index.get_loc(current, method='nearest')
        oneYearDf = mdf.iloc[start:end+1]
        momentum = oneYearDf.iloc[-1] - oneYearDf
        momentumIndex = momentum / oneYearDf
        momentumScore = momentum.applymap(lambda val: 1 if val > 0 else 0 )
        return momentumIndex.mean(), momentumScore.mean()

    def get12MomentumMean(self, current, df):
        mdf = df.resample('M').mean()
        beforeMomentumDate = current + pd.Timedelta(-1, unit='Y')
        start = mdf.index.get_loc(beforeMomentumDate, method='nearest')
        end = mdf.index.get_loc(current, method='nearest')
        oneYearDf = mdf.iloc[start:end+1]
        momentum = oneYearDf.iloc[-1] - oneYearDf
        momentumScore = momentum.applymap(lambda val: 1 if val > 0 else 0 )
        # print(df[momentumScore.iloc[0] > 0])
        return df[list(momentumScore.iloc[0][momentumScore.iloc[0] > 0].index)]

    def getMomentumInvestRate(self, momentumIndex, momentumScoreMean, shareNum, cashRate, mementumLimit):
        sortValue = momentumIndex.sort_values(ascending=False)
        sortValue = sortValue.dropna()
        # print(1,sortValue.head(shareNum).index)
        # print(2,momentumScoreMean)
        share = momentumScoreMean#[list(sortValue.tail(shareNum).index)]
        distMoney = share / (share + cashRate)
        distMoney = distMoney[share > mementumLimit]
        distMoney = distMoney[sortValue.head(shareNum).index]
        sumMoney = distMoney.sum()
        return {'invest':distMoney / sumMoney, 'perMoney':sumMoney / distMoney.size if distMoney.size != 0 else 0}

    def setMonmentumRate(self, shares, current, topdf, cashRate, mNum, mUnit, mementumLimit, limit12 = False):
        momentumdf = topdf if not limit12 else self.get12MomentumMean(current, topdf)
        # print('momentum:',len(list(momentumdf.columns)))
        momentumMean, momentumScoreMean = self.getMomentumMean(current, momentumdf, mNum=mNum, mUnit=mUnit)
        intersect = list(set(momentumMean.index) & set(shares.shareList))
        rate = self.getMomentumInvestRate(momentumMean[intersect], momentumScoreMean[intersect], shareNum=shares.shareNum, cashRate=cashRate, mementumLimit=mementumLimit)
        shares.investRate = rate['invest']
        shares.perMoneyRate = rate['perMoney']
    
    def setFactorRate(self, shares, current, topdf, factordf, factor):
        targetdf = None
        factor = list(filter(lambda x : x['name']==factor, shares.factor.factors))
        if len(factor) <= 0:
            return
        factor = factor[0]
        targetdf = factordf[factor['name']][current.year - 1]
        shcodes = list(targetdf.sort_values(ascending=factor['ascending']).head(factor['num']).index)
        nameList = list(factordf[factor['name']].loc[shcodes]['종목명'])
        rate = pd.Series([1/len(nameList)]*len(nameList), index=nameList)
        shares.investRate = rate
        shares.perMoneyRate = 1

    def getMomentumList(self, shares, current, targetdf, mNum, mUnit, limit, minVal=-1):
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
        
    
    def getFactorList(self, shares, current, targetdf, factordf, factor, ascending, num, minVal=-10000000, maxVal=10000000):
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
    
    def calculateFactorList(self, shares, nameList):
        rate = pd.Series([1/len(nameList)]*len(nameList), index=nameList)
        shares.investRate = rate
        shares.perMoneyRate = 1

    def buy(self, shares, wallet, buyDate, valuedf):
        money = shares.investMoney
        rateMoney = shares.investRate * shares.investMoney
        stockWallet = wallet.stockWallet
        # stockWallet = pd.DataFrame()
        stockValue = valuedf.iloc[valuedf.index.get_loc(buyDate, method='ffill')][rateMoney.index]
        rowdf = pd.DataFrame(data=[[0]*len(rateMoney.index)], index=[buyDate], columns=rateMoney.index)
        intersect = list(set(stockWallet.columns) & set(rowdf.columns))
        if buyDate not in stockWallet.index:
            stockWallet = pd.concat([stockWallet, rowdf], axis=0, sort=False)
        else:
            if len(intersect) > 0:
                stockWallet.loc[buyDate, intersect] = 0
                rowdf = rowdf.drop(columns=intersect)
                stockWallet = pd.concat([stockWallet, rowdf], axis=1, sort=False)
            else:
                stockWallet = pd.concat([stockWallet, rowdf], axis=1, sort=False)
        #구매
        for col in rateMoney.index:
            rMoney = rateMoney[col]
            sValue = stockValue[col]
            while (rMoney - sValue) > 0:
                if col in stockWallet.columns and buyDate in stockWallet.index:
                    a = stockWallet.at[buyDate, col]
                    stockWallet.at[buyDate, col] = a + 1
                money -= sValue
                rMoney -= sValue
        wallet.stockWallet = stockWallet
        shares.restMoney += money
        print('투자리스트:',shares.name)
        print('=================================================')
        print('investNum', len(rateMoney.index), '투자금 합계: ',rateMoney.sum() - money)
        print(list(rateMoney.index))
        print('=================================================')

    def restInvestBuy(self, buyDate, valuedf, money, wallet):
        bondValue = valuedf.iloc[valuedf.index.get_loc(buyDate, method='ffill')][valuedf.columns[0]]
        stockNum = 0
        while bondValue < money:
            money -= bondValue
            stockNum += 1
        wallet.bondNum = stockNum
        wallet.restMoney = money