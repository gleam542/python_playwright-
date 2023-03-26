import pymongo


class Mongo(object):
    def __init__(self, db_host, db_port, db_name):
        url = "mongodb://{}:{}".format(db_host, db_port)
        self.client = pymongo.MongoClient(url)
        self.db = self.client[db_name]

    def find_all(self, coll, query, sort_key=None, sort_val=None):
        if sort_key is None:
            return self.db[coll].find(query)
        return self.db[coll].find(query).sort(sort_key, pymongo.DESCENDING)

    def find_one(self, coll, query):
        return self.db[coll].find_one(query)

    def insert_one(self, coll, doc):
        return self.db[coll].insert_one(doc)

    def count_docs(self, coll, query):
        return self.db[coll].count_documents(query)

    def del_docs(self, coll, query):
        return self.db[coll].delete_many(query)

    def update_one(self, coll, query, update):
        return self.db[coll].update_one(query, update, upsert=True)

    def update_many(self, coll, query, update):
        return self.db[coll].update_many(query, update, upsert=True)
