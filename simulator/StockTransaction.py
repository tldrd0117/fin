import pandas as pd

class StockTransaction:
    @staticmethod
    def create(topdf):
        st = StockTransaction()
        st.topdf = topdf
        return st
    
    def buy(self, order, startDate, endDate):
        stockValue = self.topdf.iloc[self.topdf.index.get_loc(startDate, method='ffill')][order.code]
        startValue = stockValue * order.quantity
        stockEndValue = self.topdf.iloc[self.topdf.index.get_loc(endDate, method='ffill')][order.code]
        endValue = stockEndValue * order.quantity
        return {'startValue':stockValue, 'endValue':stockEndValue, 'yield':endValue/startValue, 'quantity': order.quantity, 'rest': order.money%stockValue, 'stock':endValue}
    
    def buy(self, order, current):
        stockValue = self.topdf.iloc[self.topdf.index.get_loc(current, method='ffill')][order.code]
        return {'stock': stockValue * order.quantity, 'rest': 0 }
    
    def sell(self, order, current):
        stockValue = self.topdf.iloc[self.topdf.index.get_loc(current, method='ffill')][order.code]
        return {'stock': -stockValue * order.quantity, 'rest': stockValue * order.quantity}

    def calculateLosscutRate(self, code, current):
        target = self.topdf[code]
        currentloc = self.topdf.index.get_loc(current, method='ffill')
        before = current + pd.Timedelta(-1, unit='M')
        beforeloc = self.topdf.index.get_loc(before, method='ffill')
        beforedf = target.iloc[beforeloc:currentloc]
        return 1 - (((beforedf.max() - beforedf.min()) / beforedf.mean())*1)
       
    
    def losscut(self, order, startDate, endDate):
        startIndex = self.topdf.index.get_loc(startDate, method='ffill')
        endIndex = self.topdf.index.get_loc(endDate, method='ffill')
        index = startIndex
        cutValue = 0
        cutDate = ''
        isLosscut = False
        while index <= endIndex:
            if index == 0:
                continue
            stockValue = self.topdf.iloc[index][order.code]
            beforeValue = self.topdf.iloc[index - 1][order.code]
            yieRate = 1 + (stockValue - beforeValue)/beforeValue
            cutRate = self.calculateLosscutRate(order.code, self.topdf.index[index])
            if yieRate < cutRate:
                cutValue = stockValue
                cutDate = self.topdf.index[index]
                isLosscut = True
                break
            index += 1
        return {'isLosscut':isLosscut, 'stock': 0,'rest': stockValue * order.quantity,"cutValue":stockValue, 'cutDate': cutDate, 'code': order.code, 'quantity': order.quantity}
        


    def possibleQuantity(self, current, money, code):
        stockValue = self.topdf.iloc[self.topdf.index.get_loc(current, method='ffill')][code]
        return int(money / stockValue)
        
