class Wallet:
    @staticmethod
    def create():
        wallet = Wallet()
        wallet.stock = []
        return wallet
    
    def buy(self, code, quantity):
        self.stock.append({"code":code,"quantity":quantity})
    
    def sell(self, code, quantity):
        for stock in self.stock:
            if code == stock.code:
                stock.quantity ##수정필요