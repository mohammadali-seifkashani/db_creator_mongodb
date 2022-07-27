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


def load_data(baseaddr, existing_table_names):
    data = None
    extension = check_dir_files_type(baseaddr)

    if extension == '.txt':
        data = handle_text_data(baseaddr, existing_table_names)
    # elif extension == '.json':
    #     data = handle_json_data(baseaddr, existing_table_names)
    # elif extension in ['.csv', '.xlsx']:
    #     data = handle_excel_data(baseaddr, existing_table_names)
    # elif extension == '':
    #     data = handle_dir_data(baseaddr, existing_table_names)

    return data


def handle_text_data(baseaddr, existing_table_names):
    for filename in os.listdir(baseaddr):
        if filename == 'columns_format.json':
            continue
        elif filename in existing_table_names:
            yield False

        filename_without_extension = os.path.splitext(filename)[0]
        result = {
            'table_name': filename_without_extension,
            'rows': []
        }
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
