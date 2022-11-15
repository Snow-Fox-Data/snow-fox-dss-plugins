import dataiku
from dataiku.customrecipe import *
from dataiku import SQLExecutor2
import pandas as pd, numpy as np
import re

from rename_columns.mapping import do_map
from rename_columns.mapping import add_description

# Source
source_dataset = get_input_names_for_role('source_dataset')
source_datasets = [dataiku.Dataset(name) for name in source_dataset]

# Mappings
mapping_dataset = get_input_names_for_role('mapping_dataset')
mapping_datasets = [dataiku.Dataset(name) for name in mapping_dataset]

# Output
output_dataset = get_output_names_for_role('output_dataset')
output_datasets = [dataiku.Dataset(name) for name in output_dataset]

# Config
cfg = get_recipe_config()
source_field = cfg['source_field']
target_field = cfg['target_field']

desc_field = ''
if 'desc_field' in cfg:
    desc_field = cfg['desc_field']

table_field = ''
table_name = ''
if 'table_name' in cfg:
    table_field = get_recipe_config()['table_field']
    table_name = get_recipe_config()['table_name']

map_ds = mapping_datasets[0]
map_df = map_ds.get_dataframe()

source_ds = source_datasets[0]
output_ds = output_datasets[0]

def add_description(desc_field, output_ds, comments):
    if desc_field != '':
        ds_schema = output_ds.read_schema()
        for column in ds_schema:
            if column['name'].lower() in comments:
                column['comment'] = comments[column['name'].lower()]

        output_ds.write_schema(ds_schema)

def do_map(source_ds, output_ds, map_df, table_name, table_field, desc_field, source_field, dest_field):
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

        if len(remapped) > 0:
            row = remapped.iloc[0]

            # removing all special characters in the mappings
            new_name = column

            # if the field is empty, just use the existing column name
            if pd.isna(row[dest_field]):
                 new_name = column
            else:
                # remove puncuation
                new_name = re.sub(r'(?<=[.?!])( +|\Z)', '', row[dest_field])
                # replace non-alphanumeric characters with an empty string
                new_name = re.sub(r'[^a-zA-Z0-9_]', ' ', new_name)
                # capitalize each word
                # new_name = string.capwords(new_name)

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
            sql += f'"{column}",'

    # remove the last comma
    sql = sql[0:len(sql)-1]

    sql += ' FROM ' + source_table

    return sql, comments

# map from Field => Remap Name
sql, comments = do_map(source_ds, output_ds, map_df, table_name, table_field, desc_field, source_field, target_field)
executor = SQLExecutor2(dataset=source_ds)
executor.exec_recipe_fragment(output_ds, query = sql,overwrite_output_schema=True)

# after writing the output dataset, update the descriptions if a description is provided
add_description(desc_field, output_ds, comments)