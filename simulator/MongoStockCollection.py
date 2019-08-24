from pymongo import MongoClient

class MongoStockCollection:
    @staticmethod
    def create():
        msc = MongoStockCollection()
        msc.client = MongoClient('localhost', 27017)
        msc.db = msc.client['findata']
        msc.collection = msc.db['stock']
        return msc
    
    def get(self):
        return self.collection
    
    




