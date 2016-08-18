
#from pymongo.connection import Connection
from pymongo import MongoClient
#from bson.binary import Binary



class MongoWrapper():

    def __init__(self):
        self.client =  MongoClient("mongodb://localhost:29017/admin")
        #self.client = MongoClient("mongodb://sb-db4.swissbib.unibas.ch:29017/admin")
        #self.client = MongoClient("mongodb://localhost:29017/admin")
        self.solrDB = self.client["vftest"]
        self.collections = {
            'all': self.solrDB["all"],
            'bots': self.solrDB["bots"],
            'full': self.solrDB["full"],
            'home': self.solrDB["home"],
            'search': self.solrDB["search"],
            'sru': self.solrDB["sru"]

        }


    def getCollection(self, name):
        return self.collections[name]

    def insertLine(self,
                   collection,
                   rawLine,
                   date,
                   type):

        logEvent = {
            "date": date,
            "line": rawLine,
            "type": type
        }

        try:

            self.getCollection(collection).insert(logEvent)
        except Exception as pythonBaseException:
            print pythonBaseException

    def closeConnections(self):
        if not self.client is None:
            self.client.close()