import pymongo
from dataloader import load_data
from utils import time_decorator


class MongoDB:
    def __init__(self, dbname):
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = myclient[dbname]

    def create(self, dbname):
        pass

    def create_collection(self, collection_name):
        pass

    @time_decorator
    def fill_collection(self, collection_name, data):
        collection = self.db[collection_name]
        collection.insert_many(data)

    @time_decorator
    def fill(self, baseaddr, columns):
        existing_collection_names = self.get_collection_names()
        if 'general' in columns:
            for i, data in enumerate(load_data(baseaddr, existing_collection_names)):
                print(i, data['collection_name'])
                if not data['rows']:
                    continue
                json_data = [dict(zip(columns['general'], r)) for r in data['rows']]
                self.fill_collection(data['collection_name'], json_data)
        else:
            for i, data in enumerate(load_data(baseaddr, existing_collection_names)):
                print(i, data['collection_name'])
                if not data['rows']:
                    continue
                json_data = [dict(zip(columns[data['collection_name']], r)) for r in data['rows']]
                self.fill_collection(data['collection_name'], json_data)

    @staticmethod
    def remove(dbname):
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        myclient.drop_database(dbname)

    def get_collection_names(self):
        return self.db.list_collection_names()

    def get_collection(self, collection_name, columns=['*']):
        if columns == ['*']:
            return self.db[collection_name].find()
        else:
            columns_dict = {column: 1 for column in columns}
            columns_dict['_id'] = 0
            return self.db[collection_name].find({}, columns_dict)

    def get_collection_generator(self, collection_name, columns=['*']):
        if columns == ['*']:
            yield from self.db[collection_name].find()
        else:
            columns_dict = {column: 1 for column in columns}
            columns_dict['_id'] = 0
            yield from self.db[collection_name].find({}, columns_dict)

    def get_database_generator(self, columns):
        collection_names = self.get_collection_names()
        # self.cursor.itersize = 10000
        for i, t in enumerate(collection_names):
            print(i, t)
            yield self.get_collection(t, columns)

    def get_database(self, columns):
        result = {}
        collection_names = self.get_collection_names()
        # self.cursor.itersize = 10000
        for i, t in enumerate(collection_names):
            print(i, t)
            result[t] = self.get_collection(t, columns)

        return result

    def get_collection_to_file(self, collection_name, format):
        collection = self.get_collection(collection_name)
        if format == 'txt':
            pass
        elif format == 'json':
            pass
        elif format == 'xlsx':
            pass
        elif format == 'csv':
            pass
        elif format == 'pkl':
            pass
