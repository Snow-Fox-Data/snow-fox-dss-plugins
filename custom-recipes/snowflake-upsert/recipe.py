import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import get_input_names_for_role
from dataiku.customrecipe import get_output_names_for_role
from dataiku.customrecipe import get_recipe_config
from pandas.api.types import is_numeric_dtype
from datetime import datetime

import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku import SQLExecutor2

# Read recipe input and output
si1 = get_input_names_for_role('source_dataset')
si1 = [dataiku.Dataset(name) for name in si1]
source_ds = si1[0]

si2 = get_output_names_for_role('destination_dataset')
si2 = [dataiku.Dataset(name) for name in si2]
out_ds = si2[0]

# log file dataset (optional)
si2 = get_output_names_for_role('log_dataset')
si2 = [dataiku.Dataset(name) for name in si2]

log_ds = ''
logs_table = ''
if len(si2) > 0:
    log_ds = si2[0]
    
    logs_table = log_ds.get_location_info()['info']['table']

# plugin config

cfg = get_recipe_config()
key_field = cfg['key_name']
mode = cfg['mode_selection']

tm = str(datetime.now())
log = {'sync_date': tm, 'notes': ''}
    
# does the schema of the output exit?
schema_exists = True
try:
    out_ds.read_schema()
except Exception as e:
    print(f'output dataset schema does not exist {e}')
    schema_exists = False

def get_dataset_info(ds):
    source_schema = ''
    source_db = ''
    source_table = ds.get_location_info()['info']['table']
    source_table_full = source_table
    if 'schema' in ds.get_location_info()['info']:
        source_schema = ds.get_location_info()['info']['schema']
        source_table_full = source_schema + '.' + source_table

    if 'catalog' in ds.get_location_info()['info']:
        source_db = ds.get_location_info()['info']['catalog']
        
        source_table_full = source_db + '.' + source_table_full
        
    return source_db, source_schema, source_table, source_table_full

source_db, source_schema, source_table, source_table_full = get_dataset_info(source_ds)
out_db, out_schema, out_table, out_table_full = get_dataset_info(out_ds)

output_table_exists = False
# are there any records in the output table?
if schema_exists:
    try:
        executor = SQLExecutor2(dataset=out_ds)
        
        qry = 'SHOW TABLES'
        if out_schema != '' or out_db != '':
            if out_db != '' and out_schema != '':
                qry += ' IN ' + out_db + '.' + out_schema
            else:
                qry += ' IN ' + out_schema
        
        print('searching for table')
        print(qry)

        query_reader = executor.query_to_iter(qry)
        query_iterator = query_reader.iter_tuples()
        for row in query_iterator:
            if out_table == row[1] and (out_schema == '' or out_schema == row[3]) and (out_db == '' or out_db == row[2]):
                output_table_exists = True

                print(f'output table {out_table} already exists')
                break

    except Exception as e:
        output_table_exists = False
        print(f'couldnt select from output. error: {e}')

if output_table_exists:
    cols = ''
    dest_cols = ''
    for c in source_ds.read_schema():
        cols += '"' + c['name'] + '",'
        dest_cols += 'st."' + c['name'] + '",'
    cols = cols[0:-1]
    dest_cols = dest_cols[0:-1]
    
    if "key" in mode:
        print('in key mode')
        # find all changes
        sql = f'SELECT COUNT(*) FROM (SELECT {cols} FROM {source_table_full} EXCEPT SELECT {cols} FROM {out_table_full})'
        executor = SQLExecutor2(dataset=out_ds)
        update_df = executor.query_to_df(sql)
                
        # find insert keys
        sql = f'SELECT COUNT(*) FROM {out_table_full} ot RIGHT JOIN {source_table_full} st ON ot."{key_field}"=st."{key_field}" WHERE ot."{key_field}" IS NULL'

        executor = SQLExecutor2(dataset=out_ds)
        missing_df = executor.query_to_df(sql)
        print(sql)
        
        if missing_df.iloc[0][0] > 0:
            sql_insert = f'INSERT INTO {out_table} ({cols}) '
            sql_select = f'SELECT {cols} FROM {source_table_full} WHERE "{key_field}" IN (SELECT st."{key_field}" FROM {out_table_full} ot RIGHT JOIN {source_table_full} st ON ot."{key_field}"=st."{key_field}" WHERE ot."{key_field}" IS NULL)'

            print(sql_insert)
            print(sql_select)
            
            executor = SQLExecutor2(dataset=out_ds)
            result_df = executor.query_to_df(sql_insert + sql_select, post_queries=['COMMIT'])
        
        if mode == 'key_update':
            update_ct = update_df.iloc[0][0] - missing_df.iloc[0][0]
            if update_ct > 0:
                # this is actually deleting and re-inserting rather than updating
                print('updating...')
                sql1 = f'CREATE TEMPORARY TABLE tmp_updates as (SELECT {cols} FROM {source_table_full} EXCEPT SELECT * FROM {out_table_full})'
                sql2 = f'DELETE FROM {out_table_full} WHERE "{key_field}" in (SELECT "{key_field}" FROM tmp_updates)'
                sql = f'INSERT INTO {out_table_full}  SELECT {cols} FROM tmp_updates'
                sql3 = 'DROP TABLE tmp_updates'

                print(sql)
                res_df = executor.query_to_df(sql, pre_queries=[sql1, sql2], post_queries=['COMMIT', sql3])
        
            log['notes'] = f'inserts: {missing_df.iloc[0][0]}; updates: {update_ct}'
            log['affected_records'] = update_df.iloc[0][0]
        else:
            log['notes'] = f'inserts: {missing_df.iloc[0][0]}'
            log['affected_records'] = missing_df.iloc[0][0]
            
    else:
        # range mode
        sql = f'SELECT MAX("{key_field}") from {out_table_full}'
        executor = SQLExecutor2(dataset=out_ds)
        max_df = executor.query_to_df(sql)   
        print(sql)
        
        max_val = max_df.iloc[0][0]
        numeric_type = False
    
        try:
            numeric_type = is_numeric_dtype(max_val)
        except:
            numeric_type = False

        if not numeric_type:
            max_val = f"'{max_val}'"
            
        print(f'found max value of {max_val}')
        
        sql_insert = f'INSERT INTO {out_table_full} ({cols}) '
        sql_select = f'SELECT {cols} FROM {source_table_full} WHERE "{key_field}" > {max_val}'
        
        sql_log = f'SELECT COUNT(*) FROM {source_table_full} WHERE "{key_field}" > {max_val}'
        executor = SQLExecutor2(dataset=source_ds)
        log_result_df = executor.query_to_df(sql_log)
        log['affected_records'] = log_result_df.iloc[0][0]
        log['notes'] = f'max existing val: {max_val}'

        # execute the insert
        executor = SQLExecutor2(dataset=out_ds)
        result_df = executor.query_to_df(sql_insert + sql_select, post_queries=['COMMIT'])      
else:
    # table doesn't exist, just "sync" from source=>dest
    sql = f'CREATE TABLE {out_table_full} AS SELECT * FROM {source_table_full}'
    print(sql)
    executor = SQLExecutor2(dataset=out_ds)
    res = executor.query_to_df(sql)
    
    if not schema_exists:
        out_ds.write_schema_from_dataframe(source_ds.get_dataframe(limit=1))
    
    sql_log = f'SELECT COUNT(*) FROM {source_table_full}'
    executor = SQLExecutor2(dataset=source_ds)
    log_result_df = executor.query_to_df(sql_log)
    log['affected_records'] = log_result_df.iloc[0][0]
    log['notes'] = 'full sync'
    
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
