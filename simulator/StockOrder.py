class StockOrder:
    @staticmethod
    def create(code, quantity, money):
        stockOrder = StockOrder()
        stockOrder.code = code
        stockOrder.quantity = quantity
        stockOrder.money = money
        return stockOrder
    