import pymongo

from dataloader import load_data
from utils import time_decorator


class MongoDB:
    def __init__(self, dbname):
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = myclient[dbname]

    def create(self, dbname):
        pass
        # myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        # self.db = myclient[dbname]

    def create_collection(self, collection_name):
        pass
        # table = self.db[collection_name]

    @time_decorator
    def fill_table(self, collection_name, data):
        table = self.db[collection_name]
        table.insert_many(data)

    @time_decorator
    def fill(self, baseaddr, columns):
        existing_table_names = self.get_collection_names()
        if 'general' in columns:
            for i, data in enumerate(load_data(baseaddr, existing_table_names)):
                print(i, data['table_name'])
                if not data:
                    continue
                json_data = [dict(zip(columns['general'], r)) for r in data['rows']]
                self.fill_table(data['table_name'], json_data)
        else:
            for i, data in enumerate(load_data(baseaddr, existing_table_names)):
                print(i, data['table_name'])
                if not data:
                    continue
                json_data = [dict(zip(columns[data['table_name']], r)) for r in data['rows']]
                self.fill_table(data['table_name'], json_data)

    @staticmethod
    def remove(dbname):
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        myclient.drop_database(dbname)

    def get_collection_names(self):
        return self.db.list_collection_names()

    def get_collection(self, collection_name, columns=['*']):
        if columns == ['*']:
            yield from self.db[collection_name].find()
        else:
            columns_dict = {column:1 for column in columns}
            columns_dict['_id'] = 0
            yield from self.db[collection_name].find({}, columns_dict)

    def get_database(self, columns):
        result = {}
        collection_names = self.get_collection_names()
        # self.cursor.itersize = 10000
        for i, t in enumerate(collection_names):
            print(i, t)
            yield self.get_collection(t, columns)
