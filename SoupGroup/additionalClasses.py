class RelationalProcessor(object):
    def __init__(self):
        self.dbPath = ""
    
    def getDbPath(self):
        return self.dbPath
    
    def setDbPath(self, newDbPath):
        self.dbPath = newDbPath
        return True


class QueryProcessor(object): 
    def __init__(self):
        pass


class TriplestoreProcessor(object):
    def __init__(self):
        self.endpointUrl = ""
    
    def getEndpointUrl(self):
        return self.endpointUrl

    def setEndpointUrl(self, url):
        if type(url) == str:
            self.endpointUrl = url
            return True
        else:
            return False