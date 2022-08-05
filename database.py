import os

import pandas as pd
import pymongo
from dataloader import load_data
from utils import time_decorator, save_json, save_pickle


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
        if collection_name not in self.get_collection_names():
            raise Exception('collection does not exist.')
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

    def get_database(self, columns):
        result = {}
        collection_names = self.get_collection_names()
        # self.cursor.itersize = 10000
        for i, t in enumerate(collection_names):
            print(i, t)
            result[t] = self.get_collection(t, columns)

        return result

    def get_database_generator(self, columns):
        collection_names = self.get_collection_names()
        # self.cursor.itersize = 10000
        for i, t in enumerate(collection_names):
            print(i, t)
            yield self.get_collection(t, columns)

    def collection_to_file(self, collection_name, out_address, key_field):
        collection = self.get_collection(collection_name)
        extension = os.path.splitext(out_address)[1]
        if extension == '.txt':
            result = ''
            for row in collection:
                row.pop('_id')
                row_without_None = ['' if i is None else i for i in list(row.values())[1:]]
                result += '\t'.join(row_without_None) + '\n'
            f = open(out_address, 'w')
            f.write(result)
            f.close()
        elif extension == '.json':
            result = {}
            for row in collection:
                row.pop('_id')
                result[row[key_field]] = row
            save_json(result, out_address)
        elif extension == '.xlsx':
            d = {}
            for row in collection:
                row.pop('_id')
                d[row[key_field]] = row
            df = pd.DataFrame.from_dict(d, orient='index', columns=list(row.keys()))
            df.to_excel(out_address)
        elif extension == '.csv':
            d = {}
            for row in collection:
                row.pop('_id')
                d[row[key_field]] = row
            df = pd.DataFrame.from_dict(d, orient='index', columns=list(row.keys()))
            df.to_csv(out_address)
