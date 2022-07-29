import os
import pymongo
from database import MongoDB
from utils import load_json, load_pickle

geodb = MongoDB('test')
# baseaddr = 'C:/COMMON/work/geonames/main/'
# columns = load_json(os.path.join(baseaddr, 'columns_format.json'))
# geodb.fill(baseaddr, columns)
geodb.collection_to_file('AD', 'C:/COMMON/work/geonames/ad.pkl', 'geonameid')
# collection = load_pickle('C:/COMMON/work/geonames/ad.pkl')
# for x in collection:
#     print(x)
