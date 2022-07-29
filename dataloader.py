import os
import pandas as pd
from detect_delimiter import detect
from utils import load_json


def check_dir_files_type(baseaddr):
    all_folder = None

    for f in os.listdir(baseaddr):
        if os.path.isfile(os.path.join(baseaddr, f)):
            if all_folder == True:
                raise Exception('directory can not have both directories and files')
            all_folder = False
        else:
            if all_folder == False:
                raise Exception('directory can not have both directories and files')
            all_folder = True

    if all_folder:
        extension = ''
    else:
        f = os.listdir(baseaddr)[0]
        extension = os.path.splitext(f)[1].lower()

    return extension


def load_data(baseaddr, existing_column_names):
    data = None
    extension = check_dir_files_type(baseaddr)

    if extension == '.txt':
        data = handle_text_data(baseaddr, existing_column_names)
    elif extension == '.json':
        data = handle_json_data(baseaddr, existing_column_names)
    elif extension in ['.csv', '.xlsx']:
        data = handle_excel_data(baseaddr, existing_column_names)
    elif extension == '':
        data = handle_dir_data(baseaddr, existing_column_names)

    return data


def handle_text_data(baseaddr, existing_column_names):
    for filename in os.listdir(baseaddr):
        filename_without_extension = os.path.splitext(filename)[0]
        result = {
            'collection_name': filename_without_extension,
            'rows': []
        }
        if filename_without_extension == 'columns_format':
            continue
        elif filename_without_extension in existing_column_names:
            yield result
            continue

        f = open(os.path.join(baseaddr, filename), 'r', encoding='utf8')
        lines = f.read().strip('\n').split('\n')
        delimiter = detect(lines[0], whitelist=['\t', ',', ' '])

        for line in lines:
            row_list = line.split(delimiter)
            for i in range(len(row_list)):
                if row_list[i] == '':
                    row_list[i] = None
            result['rows'].append(row_list)

        yield result


def handle_json_data(baseaddr, existing_column_names):
    for filename in os.listdir(baseaddr):
        filename_without_extension = os.path.splitext(filename)[0]
        result = {
            'collection_name': filename_without_extension,
            'rows': []
        }
        if filename_without_extension == 'columns_format':
            continue
        elif filename_without_extension in existing_column_names:
            yield result
            continue

        d = load_json(os.path.join(baseaddr, filename))
        for i in range(len(d)):
            for key in d[i]:
                if d[i][key] == '':
                    d[i][key] = None
            result['rows'].append(list(d[i].values()))

        yield result


def handle_excel_data(baseaddr, existing_column_names):
    for filename in os.listdir(baseaddr):
        filename_without_extension = os.path.splitext(filename)[0]
        result = {
            'collection_name': filename_without_extension,
            'rows': []
        }
        if filename_without_extension == 'columns_format':
            continue
        elif filename_without_extension in existing_column_names:
            yield result
            continue

        filename_extension = os.path.splitext(filename)[1]
        if filename_extension == '.xlsx':
            df = pd.read_excel(os.path.join(baseaddr, filename))
        else:
            df = pd.read_csv(os.path.join(baseaddr, filename))
        result['rows'] = df.values.tolist()
        yield result


def handle_dir_data(baseaddr, existing_column_names):
    for collection_name in os.listdir(baseaddr):
        if collection_name == 'columns_format.json':
            continue
        elif collection_name in existing_column_names:
            yield False

        result = {
            'collection_name': collection_name,
            'rows': []
        }
        for filename_id in os.listdir(os.path.join(baseaddr, collection_name)):
            for filename in os.listdir(os.path.join(baseaddr, collection_name, filename_id)):
                if filename == f'{filename_id}.txt':
                    data = handle_text_data(
                        os.path.join(baseaddr, collection_name, filename_id, filename), existing_column_names)
                    row = data['rows'][0]
                    break
                elif filename == f'{filename_id}.json':
                    data = handle_json_data(
                        os.path.join(baseaddr, collection_name, filename_id, filename), existing_column_names)
                    row = data['rows'][0]
                    break
                elif filename in [f'{filename_id}.csv', f'{filename_id}.xlsx']:
                    data = handle_excel_data(
                        os.path.join(baseaddr, collection_name, filename_id, filename), existing_column_names)
                    row = data['rows'][0]
                    break

            result['rows'].append(row)

        yield result
