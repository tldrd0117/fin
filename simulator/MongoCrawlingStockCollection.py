from pymongo import MongoClient

class MongoAssetCollection:
    @staticmethod
    def create():
        mac = MongoAssetCollection()
        mac.client = MongoClient('localhost', 27017)
        mac.db = mac.client['crawling']
        mac.collection = mac.db['stock']
        return mac
    
    def get(self):
        return self.collection
    
    




