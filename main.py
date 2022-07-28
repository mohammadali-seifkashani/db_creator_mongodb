from database import MongoDB


def main():
    geodb = MongoDB('test')
    geodb.create_collection('a')
    print(geodb.db.list_collection_names())


if __name__ == '__main__':
    main()