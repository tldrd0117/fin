class Wallet:
    @staticmethod
    def create():
        wallet = Wallet()
        wallet.stock = []
        return wallet
    def clear(self):
        self.stock = []
    
    def buy(self, code, quantity, money):
        stock = self.getStock(code)
        # if stock:
        #     stock['quantity'] += 1
        # else:
        self.stock.append({"code":code,"quantity":quantity, 'money': money})
    
    def sell(self, code, quantity, money):
        stock = self.getStock(code)
        if stock:
            print(stock['quantity'],quantity, stock['code'])
            if stock['quantity'] < quantity: 
                print('주식수량부족')
                return False
            if self.getStockTotalQuantity(stock['code']) <= 0:
                print('팔 주식 없음')
                return False
            self.stock.append({"code":code,"quantity":-quantity, 'money': money})
            return True
        return False
    def getStock(self, code):
        for stock in self.stock:
            if code == stock['code']:
                return stock
        return None
    def getStockLastMoney(self, code):
        targetStock = None
        for stock in self.stock:
            if code == stock['code']:
                targetStock = stock
                break
        if targetStock:
            return targetStock['money']
        else:
            return None
        

    def getStockRatio(self, code):
        targetStock = None
        for stock in self.stock:
            if code == stock['code']:
                targetStock = stock
                break
        if 'ratio' in targetStock:
            return targetStock['ratio']
        else:
            return 1

    def getStockTotalQuantity(self, code):
        results = []
        for stock in self.stock:
            if code == stock['code']:
                results.append(stock)
        quantity = 0
        for result in results:
            quantity += result['quantity']
        return quantity

    def getAllStock(self):
        return self.stock
