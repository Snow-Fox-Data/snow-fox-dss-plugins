import string
import dataiku
from dataiku.customrecipe import *
from dataiku import SQLExecutor2
import pandas as pd, numpy as np
import re

def char_replacements(col_name, to_upper, space_replace, special_char_replace, dollar_char_replace, start_char_replace):
    new_name = col_name

    if space_replace != 'no':
        if space_replace == 'underscore':
            new_name = new_name.replace(' ', '_')
        else:
            new_name = new_name.replace(' ', '')

    if dollar_char_replace != 'no':
        if dollar_char_replace == 'underscore':
            new_name = new_name.replace('$', '_')
        else:
            new_name = new_name.replace('$', '')

    if start_char_replace != 'no':
        if start_char_replace == 'underscore':
            if new_name[0].isdigit():
                new_name = '_' + new_name
        else:
            while new_name[0].isdigit():
                new_name = new_name[1:]
    
    # if the field is empty, just use the existing column name
    if special_char_replace != 'no':
        replace_char = '_'
        if special_char_replace == 'delete':
            replace_char = ''
        
        # remove puncuation
        new_name = re.sub(r'(?<=[.?!])(+|\Z)', replace_char, new_name)
        # replace non-alphanumeric characters 
        # new_name = re.sub(r'[^a-zA-Z0-9_]', replace_char, new_name)
        
    # remove trailing underscore
    if new_name.endswith('_'):
        new_name = new_name.rstrip(new_name[-1])

    if str(to_upper) == 'True':
        # capitalize each word
        new_name = new_name.upper()

    return new_name

def do_map(source_ds, output_ds, map_df, table_name, table_field, desc_field, source_field, dest_field, to_upper, space_replace, special_char_replace, dollar_char_replace, start_char_replace):
    comments = {}

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

        if len(remapped) > 0:
            row = remapped.iloc[0]

            # the mapped renaming
            if not pd.isna(row[dest_field]):
                new_name = row[dest_field]

            # handling duplicate column names
            if new_name in new_names:
                new_names[new_name] += 1
                new_name += '_' + str(new_names[new_name])
            else:
                new_names[new_name] = 1

            if desc_field != '':
                comments[new_name.lower()] = row[desc_field]

            new_name = char_replacements(new_name, to_upper, to_upper, space_replace, special_char_replace, dollar_char_replace, start_char_replace)           
            # the actual SQL renaming
            sql += '"' + row[source_field] + '" AS "' + new_name + '",'

            remapped_cols.append(row[source_field])
        else:
            new_name = char_replacements(new_name, to_upper, to_upper, space_replace, special_char_replace, dollar_char_replace, start_char_replace)           
            print(f'No mapping found for {column}')
            sql += f'"{column}" AS "{new_name}",'

    # remove the last comma
    sql = sql[0:len(sql)-1]

    sql += ' FROM ' + source_table

    return sql, comments