import string
import dataiku
from dataiku.customrecipe import *
from dataiku import SQLExecutor2
import pandas as pd, numpy as np
import re

def do_map(source_ds, output_ds, map_df, table_name, table_field, desc_field, source_field, dest_field, to_upper, char_replace_mode):
    comments = {}

    print(f'to_upper: {to_upper}, char_replace:{ char_replace_mode}')

    source_info = source_ds.get_location_info()['info']
    out_info = output_ds.get_location_info()['info']

    source_table = source_info['table']

    if 'schema' in source_info:
        source_table = source_info['schema'] + '.' + source_table

    if 'catalog' in source_info:
        source_table = source_info['catalog'] + '.' + source_table

    executor = SQLExecutor2(dataset=source_ds)

    # retrieving the source columns
    source_qry = 'SELECT TOP 1 * FROM ' + source_table
    df = executor.query_to_df(source_qry)

    sql = 'SELECT '
    remapped_cols = []
    new_names = {}

    for column in df.columns:
        qry = f'{source_field}=="{column}"'
        if table_name != '':
            qry += f' & ({table_field}=="{table_name}")'

        remapped = map_df.query(qry)

        new_name = column
        if str(to_upper) == 'True':
            # capitalize each word
            new_name = string.capwords(column)

        if len(remapped) > 0:
            row = remapped.iloc[0]

            # if the field is empty, just use the existing column name
            if not pd.isna(row[dest_field]):
                if char_replace_mode != 'no':
                    replace_char = '_'
                    if char_replace_mode == 'delete':
                        replace_char = ''
                    
                    # remove puncuation
                    new_name = re.sub(r'(?<=[.?!])( +|\Z)', replace_char, row[dest_field])
                    # replace non-alphanumeric characters 
                    new_name = re.sub(r'[^a-zA-Z0-9_]', replace_char, new_name)
                    

            # remove trailing underscore
            if new_name.endswith('_'):
                new_name = new_name.rstrip(new_name[-1])

            # handling duplicate column names
            if new_name in new_names:
                new_names[new_name] += 1
                new_name += '_' + str(new_names[new_name])
            else:
                new_names[new_name] = 1

            if desc_field != '':
                comments[new_name.lower()] = row[desc_field]

            # the actual SQL renaming
            sql += '"' + row[source_field] + '" AS "' + new_name + '",'

            remapped_cols.append(row[source_field])
        else:
            print(f'No mapping found for {column}')
            sql += f'"{column} AS {new_name}",'

    # remove the last comma
    sql = sql[0:len(sql)-1]

    sql += ' FROM ' + source_table

    return sql, comments