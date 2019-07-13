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
        return {'startValue':startValue, 'endValue':endValue, 'yield':endValue/startValue, 'quantity': order.quantity, 'rest': order.money%stockValue, 'stock':endValue*order.quantity}
    
    def possibleQuantity(self, current, money, code):
        stockValue = self.topdf.iloc[self.topdf.index.get_loc(current, method='ffill')][code]
        return int(money / stockValue)
        
