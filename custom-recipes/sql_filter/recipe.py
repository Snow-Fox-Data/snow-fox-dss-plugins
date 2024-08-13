import dataiku

from dataiku.customrecipe import get_input_names_for_role
from dataiku.customrecipe import get_output_names_for_role
from dataiku.customrecipe import get_recipe_config
from pandas.api.types import is_numeric_dtype
from datetime import datetime

import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku import SQLExecutor2

# Read recipe inputs
si1 = get_input_names_for_role('source_dataset')
si1 = [dataiku.Dataset(name) for name in si1]
      
source_ds = si1[0]

cfg = get_recipe_config()
si2 = get_input_names_for_role('destination_dataset')
si2 = [dataiku.Dataset(name) for name in si2]

dest_ds = ''
# if 'destination_dataset_name' in cfg:
if len(si2) > 0:
    dest_ds = si2[0]
#     dest_ds = dataiku.Dataset(cfg['destination_dataset_name'])

si3 = get_output_names_for_role('output_dataset')
si3 = [dataiku.Dataset(name) for name in si3]
out_ds = si3[0]

si2 = get_output_names_for_role('log_dataset')
si2 = [dataiku.Dataset(name) for name in si2]

log_ds = ''
if len(si2) > 0:
    log_ds = si2[0]

schema_exists = True
# does the schema of the output exit?
try:
    out_ds.read_schema()
except Exception as e:
    print(f'output dataset schema does not exist {e}')
    schema_exists = False

source_schema = ''
source_table = source_ds.get_location_info()['info']['table']
if 'schema' in source_ds.get_location_info()['info']:
    source_schema = source_ds.get_location_info()['info']['schema']
    source_table = source_schema + '.' + source_table
    
if 'catalog' in source_ds.get_location_info()['info']:
    source_db = source_ds.get_location_info()['info']['catalog']
        
    source_table = source_db + '.' + source_table

out_table = out_ds.get_location_info()['info']['table']
out_table_base = out_table
out_schema = ''
if 'schema' in out_ds.get_location_info()['info']:
    out_schema = out_ds.get_location_info()['info']['schema']
    out_table = out_schema + '.' + out_table
    
out_db = ''
if 'catalog' in out_ds.get_location_info()['info']:
    out_db = out_ds.get_location_info()['info']['catalog']
    out_table = out_db + '.' + out_table
    
if dest_ds != '':
    dest_table = dest_ds.get_location_info()['info']['table']
    dest_table_base = dest_table
    out_schema = ''
    if 'schema' in dest_ds.get_location_info()['info']:
        dest_schema = dest_ds.get_location_info()['info']['schema']
        dest_table = dest_schema + '.' + dest_table

    dest_db = ''
    if 'catalog' in dest_ds.get_location_info()['info']:
        dest_db = dest_ds.get_location_info()['info']['catalog']
        dest_table = dest_db + '.' + dest_table

if log_ds != '':
    logs_table = log_ds.get_location_info()['info']['table']

key_field = ''
if 'key_name' in cfg:
    key_field = cfg['key_name']
    
source_key_field = ''
if 'source_key_name' in cfg:
    source_key_field = cfg['source_key_name']

tm = str(datetime.now())
log = {'sync_date': tm, 'notes': ''}
# if table_exists:
cols = ''
dest_cols = ''
for c in source_ds.read_schema():
    cols += '"' + c['name'] + '",'
    dest_cols += 'st."' + c['name'] + '",'
cols = cols[0:-1]
dest_cols = dest_cols[0:-1]

sql_to_execute = ''
if dest_ds != '':
    sql = f'SELECT MAX("{key_field}") from {dest_table}'
    executor = SQLExecutor2(dataset=dest_ds)
    max_df = executor.query_to_df(sql)   
    print(sql)
    max_val = max_df.iloc[0][0]
    print(max_val)

    numeric_type = False
    try:
        numeric_type = is_numeric_dtype(max_val)
    except:
        numeric_type = False
    
    if not numeric_type:
        max_val = f"'{max_val}'"

    print(f'found max value of {max_val}')

    # sql_insert = f'INSERT INTO {out_table} ({cols}) '
    sql_to_execute = f'CREATE OR REPLACE TABLE {out_table} AS SELECT {cols} FROM {source_table} WHERE "{source_key_field}" > {max_val}'

    sql_log = f'SELECT COUNT(*) FROM {source_table} WHERE "{source_key_field}" > {max_val}'

    executor = SQLExecutor2(dataset=source_ds)
    
    log_result_df = executor.query_to_df(sql_log)
    log['affected_records'] = log_result_df.iloc[0][0]
    log['notes'] = f'max existing val: {max_val}'

else:
    sql_to_execute = f'CREATE OR REPLACE TABLE {out_table} AS SELECT * FROM {source_table}'
    print('PERFORMING FULL SYNC')

executor = SQLExecutor2(dataset=out_ds)
print(sql_to_execute)
res = executor.query_to_df(sql_to_execute)

# write logs
if log_ds != '':
    print('writing logs')
    exists = True
    records = []
    try:
        log_ds.read_schema()
        log_ds.spec_item["appendMode"] = True

        with log_ds.get_writer() as writer:
            append_df = pd.DataFrame(data=[log])
            writer.write_dataframe(append_df)

        print('appending to log table')
    except Exception as e:
        print('no existing log table')
        exists = False
        records = [log]

        df = pd.DataFrame.from_dict(records)
        df['sync_date'] = df.sync_date.astype(str)
        log_ds.write_with_schema(df)
