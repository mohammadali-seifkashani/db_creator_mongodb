import os.path

from database import MongoDB
from utils import load_json


def main():
    geodb = MongoDB('test')
    baseaddr = 'C:/COMMON/work/geonames/main/'
    columns = load_json(os.path.join(baseaddr, 'columns_format.json'))
    geodb.fill(baseaddr, columns)


if __name__ == '__main__':
    main()