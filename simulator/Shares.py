class Shares:
    @staticmethod
    def create(name, shareList, moneyRate):
        shares = Shares()
        shares.name = name
        shares.shareList = shareList
        shares.moneyRate = moneyRate
        return shares
    
    @staticmethod
    def toNameList(dictList):
        return list(set([ dic['Name'] for dic in dictList ]))