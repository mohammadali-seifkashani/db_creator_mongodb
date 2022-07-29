from time import time
import json


def save_json(obj, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=3)


def load_json(address):
    with open(address, 'r', encoding='utf-8') as f:
        return json.load(f)


def time_decorator(func):
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        print('spent time:', time() - t1)
        return result

    return wrap_func
