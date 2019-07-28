import pandas as pd
import numpy as np
class StockTransaction:
    @staticmethod
    def create(topdf):
        st = StockTransaction()
        st.topdf = topdf
        return st
    
    # def buy(self, order, startDate, endDate):
    #     stockValue = self.topdf.iloc[self.topdf.index.get_loc(startDate, method='ffill')][order.code]
    #     startValue = stockValue * order.quantity
    #     stockEndValue = self.topdf.iloc[self.topdf.index.get_loc(endDate, method='ffill')][order.code]
    #     endValue = stockEndValue * order.quantity
    #     return {'startValue':stockValue, 'endValue':stockEndValue, 'yield':endValue/startValue, 'quantity': order.quantity, 'rest': order.money%stockValue, 'stock':endValue}
    
    def buy(self, order, current):
        stockValue = self.topdf.iloc[self.topdf.index.get_loc(current, method='ffill')][order.code]
        return {'stock': stockValue * order.quantity, 'rest': -stockValue * order.quantity, 'code': order.code }
    
    # def goTime(self, moneyOrder, current):
    #     stockMoney = 0
    #     restMoney = 0
    #     for order in moneyOrder:
    #         if order
    #         curValue = self.topdf.iloc[self.topdf.index.get_loc(current, method='ffill')][order['code']]
    #         stockMoney = order['stock']
    #     for stock in wallet.getAllStock():
    #         money += curValue * stock['quantity']
    #     return money
    
    def sell(self, order, current):
        stockValue = self.topdf.iloc[self.topdf.index.get_loc(current, method='ffill')][order.code]
        return {'stock': -stockValue * order.quantity, 'rest': stockValue * order.quantity, 'code': order.code}

    def calculateLosscutRate(self, code, current):
        target = self.topdf[code]
        currentloc = self.topdf.index.get_loc(current, method='ffill')
        before = current + pd.Timedelta(-1, unit='M')
        # before = current + pd.Timedelta(-90, unit='D')
        beforeloc = self.topdf.index.get_loc(before, method='ffill')
        beforedf = target.iloc[beforeloc:currentloc]
        return 1 - (((beforedf.max() - beforedf.min()) / beforedf.mean())*0.5)
    
    def losscut(self, code, current, buyDate):
        currentIndex = self.topdf.index.get_loc(current, method='ffill')
        beforeIndex = self.topdf.index.get_loc(buyDate, method='ffill')
        if beforeIndex < 0:
            return False
        stockValue = self.topdf.iloc[currentIndex][code]
        beforeValue = self.topdf.iloc[beforeIndex][code]
        yieRate = 1 + (stockValue - beforeValue)/beforeValue
        cutRate = self.calculateLosscutRate(code, self.topdf.index[currentIndex])
        return yieRate < cutRate
    
    def losscutScalar(self, code, current, buyDate, cutRate):
        currentIndex = self.topdf.index.get_loc(current, method='ffill')
        beforeIndex = self.topdf.index.get_loc(buyDate, method='ffill')
        if beforeIndex < 0:
            return False
        stockValue = self.topdf.iloc[currentIndex][code]
        beforeValue = self.topdf.iloc[beforeIndex][code]
        yieRate = 1 + (stockValue - beforeValue)/beforeValue
        # cutRate = self.calculateLosscutRate(code, self.topdf.index[currentIndex])
        return yieRate < cutRate
    
    def losscutMeanVal(self, code, current, targetdf):
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
        mdf = mdf[mdf<0]
        return code in list(mdf.index)
    
    def losscutRsi(self, stocks, current, targetdf):
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

        beforeIndex = list(beforeRsi[beforeRsi >= 70].sort_values(ascending=True).index)
        curIndex = list(rsi[rsi < 65].sort_values(ascending=False).index)
        return list(filter(lambda stock : stock['code'] in list(set(beforeIndex) & set(curIndex)), stocks))
        # return code in list(set(beforeIndex) & set(curIndex))
        # return code in list(rsi[rsi >= 70].index)
    
    def getValue(self, current, code):
        return self.topdf.iloc[self.topdf.index.get_loc(current, method='ffill')][code]
        
        

       
    
    # def losscut(self, order, startDate, endDate):
    #     startIndex = self.topdf.index.get_loc(startDate, method='ffill')
    #     endIndex = self.topdf.index.get_loc(endDate, method='ffill')
    #     index = startIndex
    #     cutValue = 0
    #     cutDate = ''
    #     isLosscut = False
    #     while index <= endIndex:
    #         if index == 0:
    #             continue
    #         stockValue = self.topdf.iloc[index][order.code]
    #         beforeValue = self.topdf.iloc[index - 1][order.code]
    #         yieRate = 1 + (stockValue - beforeValue)/beforeValue
    #         cutRate = self.calculateLosscutRate(order.code, self.topdf.index[index])
    #         if yieRate < cutRate:
    #             cutValue = stockValue
    #             cutDate = self.topdf.index[index]
    #             isLosscut = True
    #             break
    #         index += 1
    #     return {'isLosscut':isLosscut, 'stock': 0,'rest': stockValue * order.quantity,"cutValue":stockValue, 'cutDate': cutDate, 'code': order.code, 'quantity': order.quantity}
        


    def possibleQuantity(self, current, money, code):
        stockValue = self.topdf.iloc[self.topdf.index.get_loc(current, method='ffill')][code]
        if not np.isnan(stockValue) and money > 0:
            return int(money / stockValue)
        else:
            return None
        
