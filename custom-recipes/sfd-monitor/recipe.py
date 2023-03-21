from dataiku import SQLExecutor2
import dataiku
from dataiku.customrecipe import *
import pandas as pd
import numpy as np
from dataiku import pandasutils as pdu
from datetime import datetime, timezone, timedelta
# import snowflake.connector
from dataiku import SQLExecutor2
import time
import json
import os
import psutil
import traceback
from sentry_sdk import capture_exception
from sentry_sdk import capture_message
import sentry_sdk

sentry_sdk.init(
    dsn="https://1b4135fb793649efa9548b0f588583b0@o1303348.ingest.sentry.io/4504734995775488",

    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    # We recommend adjusting this value in production.
    traces_sample_rate=1.0
)

# Output
error_output_dataset = get_output_names_for_role('error_output')
error_output_datasets = [dataiku.Dataset(name) for name in error_output_dataset]
error_output_ds = None
if len(error_output_datasets) > 0:
    error_output_ds = error_output_datasets[0]

metric_output_dataset = get_output_names_for_role('metric_output')
metric_output_datasets = [dataiku.Dataset(name) for name in metric_output_dataset]
if len(metric_output_datasets) > 0:
    metric_output_ds = metric_output_datasets[0]
else: 
    metric_output_ds = None

# input
dss_commit = get_input_names_for_role('dss_commits')
dss_commit_df = None
if len(dss_commit) > 0:
    dss_commits = [dataiku.Dataset(name) for name in dss_commit]
    dss_commit_df = dss_commits[0].get_dataframe()

dss_job = get_input_names_for_role('dss_jobs')
dss_jobs_df = None
if len(dss_job) > 0:
    dss_jobs = [dataiku.Dataset(name) for name in dss_job]
    dss_jobs_df = dss_jobs[0].get_dataframe()

dss_scenario = get_input_names_for_role('dss_scenarios')
dss_scenarios_df = None
if len(dss_scenario) > 0:
    dss_scenarios = [dataiku.Dataset(name) for name in dss_scenario]
    dss_scenarios_df = dss_scenarios[0].get_dataframe()

snowflake_warehouse_metering = get_input_names_for_role('snowflake_warehouse_metering')
sending_snowflake = len(snowflake_warehouse_metering) > 0
snowflake_warehouse_metering_df = None
snowflake_warehouse_meterings = []
if sending_snowflake:
    snowflake_warehouse_meterings = [dataiku.Dataset(name) for name in snowflake_warehouse_metering]
    

# Config
cfg = get_recipe_config()
client = dataiku.api_client()
proj = client.get_default_project()
p_vars = proj.get_variables()
envt = p_vars['standard']['sfd_monitor_envt']

# determining the Postgres connection
SFD_CONN_NAME = "sfd-monitor"
if "sfd_monitor_conn" in p_vars['standard']:
    SFD_CONN_NAME = p_vars['standard']['sfd_monitor_conn']

# retrieving the user used for the SFD Postgres connection
ACCT_UN = client.list_connections()[SFD_CONN_NAME]['params']['user']

# retrieving the list of metrics to collect
METRICS_TO_CHECK = p_vars['standard']['sfd_monitor_metrics']
STRING_METRICS_TO_CHECK = p_vars['standard']['sfd_monitor_string_metrics']

# grabbing the DSS Version
dss_version = json.load(open(os.path.join(
    os.environ["DIP_HOME"], "dss-version.json")))["product_version"]

vals_str = {
    'dss_version': dss_version
}

vals = {}
errors = []

def get_uptime():
    try:
        with open('/proc/uptime', 'r') as f:
            uptime_seconds = float(f.readline().split()[0])

        return uptime_seconds
    except:
        return 0

def collect_server_stats(vals, errors):
    try:
        vcpu = psutil.cpu_percent(interval=2)

        procs = psutil.cpu_percent(interval=1, percpu=True)
        proc_ct = 1
        for proc in procs:
            vals[f'cpu_{proc_ct}_pct'] = proc        
            proc_ct += 1

        # Getting % usage of virtual_memory ( 3rd field)
        vmem = psutil.virtual_memory()
        vals['cpu_total_pct'] = vcpu
        vals['ram_used_pct'] = vmem.percent
        vals['ram_used_gb'] = vmem.used/1000000000
        vals['ram_free_gb'] = vmem.free/1000000000
        vals['server_uptime_seconds'] = get_uptime()

        # disks
        disks = psutil.disk_partitions(all=False)

        for disk in disks:
            usage = psutil.disk_usage(disk.mountpoint)

            d_name = disk.mountpoint.replace('/', '_')
            if d_name == '_':
                d_name = '_root'
            vals[f'disk{d_name}_used_gb'] = usage.used/1000000000
            vals[f'disk{d_name}_free_gb'] = usage.free/1000000000
            vals[f'disk{d_name}_used_pct'] = usage.percent

    except Exception as e:
        capture_exception(e)
        errors.append({
            'type': 'system',
            'exception': traceback.format_exc()
        })

def collect_metrics(vals, vals_str, errors):
    for metric_to_check in METRICS_TO_CHECK:
        try:
            proj_name = metric_to_check.split('.')[0]
            ds_name = metric_to_check.split('.')[1]
            metric_name = metric_to_check.split('.')[2]

            project = client.get_project(proj_name)
            ds = project.get_dataset(ds_name)

            last_val = ds.get_last_metric_values().get_global_value(metric_name)

            vals[metric_to_check] = last_val
        except Exception as e:
            capture_exception(e)
            errors.append({
                'type': 'metric',
                'exception': f'{metric_to_check}: {traceback.format_exc()}',
                'date': datetime.now()
            })

    for metric_to_check in STRING_METRICS_TO_CHECK:
        try:
            proj_name = metric_to_check.split('.')[0]
            ds_name = metric_to_check.split('.')[1]
            metric_name = metric_to_check.split('.')[2]

            project = client.get_project(proj_name)
            ds = project.get_dataset(ds_name)

            last_val = ds.get_last_metric_values().get_global_value(metric_name)

            vals_str[metric_to_check] = str(last_val)
        except Exception as e:
            capture_exception(e)
            errors.append({
                'type': 'metric_string',
                'exception': f'{metric_to_check}: {traceback.format_exc()}',
                'date': datetime.now()
            })

def collect_user_project_data(vals, vals_str, errors):
    try:
        dss_users = client.list_users()

        connected_user_ct = 0
        enabled_user_ct = 0

        # Grab list of users where they have active web socket sessions
        login_list = []
        for user in dss_users:
            if user['activeWebSocketSesssions'] != 0:
                login_list.append(user['login'])
                connected_user_ct += 1
            if user['enabled']:
                enabled_user_ct += 1

        vals['dss_user_connected_count'] = connected_user_ct
        vals['dss_user_enabled_count'] = enabled_user_ct
        vals_str['dss_users_active'] = '|'.join(login_list)
        vals['dss_project_count'] = len(client.list_project_keys())

    except Exception as e:
            capture_exception(e)
            errors.append({
                'type': 'user_project',
                'exception': traceback.format_exc(),
                'date': datetime.now()
            })

collect_server_stats(vals, errors)
collect_metrics(vals, vals_str, errors)
collect_user_project_data(vals, vals_str, errors)

print(f'sending: {vals}')
print(f'sending: {vals_str}')



def insert_records(vals, vals_str, errors, dss_jobs_df, dss_commit_df, dss_scenarios_df, snowflake_warehouse_metering_df, proj):
    ts = time.time()
    utc_offset = int((datetime.fromtimestamp(ts) -
                    datetime.utcfromtimestamp(ts)).total_seconds() / 60 / 60)
    dt_string = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # ts_data
    writer = None
    try:
        qry = f"INSERT INTO dataiku.ts_data (\"account\", \"environment\", \"metric_type\", \"datetime\", \"key\", \"value_num\", \"value_str\", \"utc_offset\") VALUES "

        if metric_output_ds != None:
            metric_ds = proj.get_dataset(metric_output_ds.name.split('.')[1])
            if not metric_ds.exists() or len(metric_ds.get_schema()['columns']) == 0:
                capture_message('re-creating output metric dataset')
                metric_output_ds.write_with_schema(pd.DataFrame(columns=["datetime", "key", "value_num", "value_str", "utc_offset"]), True)                

            writer = metric_output_ds.get_writer()

        for key in vals:
            metric_type = 'SERVER'
            if ":" in key:
                metric_type = 'METRIC'

            qry += f"('{ACCT_UN}', '{envt}', '{metric_type}', '{dt_string}', '{key}', {vals[key]}, NULL, {utc_offset}),"
            if writer != None:
                writer.write_row_dict({
                    "datetime": dt_string,
                    "key": key,
                    "value_num": vals[key],
                    "value_str": '',
                    "utc_offset": utc_offset
                })

        for key in vals_str:
            metric_type = 'SERVER'
            qry += f"('{ACCT_UN}', '{envt}', '{metric_type}', '{dt_string}', '{key}', NULL, '{vals_str[key]}', {utc_offset}),"

            if writer != None:
                writer.write_row_dict({
                    "datetime": dt_string,
                    "key": key,
                    "value_num": '',
                    "value_str": vals_str[key],
                    "utc_offset": utc_offset
                })

        qry = qry[0:-1]

        print(qry)

        executor = SQLExecutor2(connection=SFD_CONN_NAME)
        executor.query_to_df(qry, post_queries=['COMMIT'])

        if writer != None:
            writer.close()

    except Exception as e:

        if writer != None:
            writer.close()

        errors.append({
            'type': 'sql_val_gen',
            'exception': traceback.format_exc(),
            'date': datetime.now()
        })
        capture_exception(e)

    # jobs
    if dss_jobs_df is not None:
        qry = ''
        try:
            tm_stmp = datetime.now() - timedelta(days=30)
            if 'sfd_monitor_dss_jobs' in p_vars['standard']:
                tm_stmp = p_vars["standard"]["sfd_monitor_dss_jobs"]

            dss_jobs_df = dss_jobs_df.query(f'time_start>"{tm_stmp}+00:00"')

            qry = f"INSERT INTO dataiku.dss_jobs (\"account\",\"environment\","
            
            col_ct = 0
            for c in dss_jobs_df.columns:
                qry += f"{c},"
            
            qry = qry[0:-1]
            qry += ') VALUES '

            for idx, row in dss_jobs_df.iterrows():
                qry += f"('{ACCT_UN}','{envt}',"

                for c in dss_jobs_df.columns:
                    qry += f"'{row[c]}',"

                qry = qry[0:-1]
                qry += '),'
            
            qry = qry.replace('\'NaT\'', 'NULL').replace('\'nan\'', 'NULL')                        
            qry = qry[0:-1]

            executor = SQLExecutor2(connection=SFD_CONN_NAME)
            executor.query_to_df(qry, post_queries=['COMMIT'])
            
            p_vars['standard']['sfd_monitor_dss_jobs'] = str(dss_jobs_df['time_start'].max()) 
        except Exception as e:
            errors.append({
                'type': 'dss_jobs',
                'exception': f'{traceback.format_exc()} | {qry}',
                'date': datetime.now()
            })   
            capture_exception(e)

    # scenarios
    if dss_scenarios_df is not None:
        qry = ''
        try:
            tm_stmp = datetime.now() - timedelta(days=30)
            if 'sfd_monitor_dss_scenarios' in p_vars['standard']:
                tm_stmp = p_vars["standard"]["sfd_monitor_dss_scenarios"]

            # only non-null rows
            dss_scenarios_df = dss_scenarios_df.query(f'time_start>"{tm_stmp}+00:00" & time_end == time_end')

            if len(dss_scenarios_df) > 0:
                qry = f"INSERT INTO dataiku.dss_scenario_runs (\"account\",\"environment\","
                
                col_ct = 0
                for c in dss_scenarios_df.columns:
                    qry += f"{c},"
                
                qry = qry[0:-1]
                qry += ') VALUES '

                for idx, row in dss_scenarios_df.iterrows():
                    qry += f"('{ACCT_UN}','{envt}',"

                    for c in dss_scenarios_df.columns:
                        sc_val = str(row[c])
                        qry += f"'{sc_val}',"
                        
                    qry = qry.replace('\'NaT\'', 'NULL').replace('\'nan\'', 'NULL')                        

                    qry = qry[0:-1]
                    qry += '),'
                
                qry = qry[0:-1]

                print(qry)

                executor = SQLExecutor2(connection=SFD_CONN_NAME)
                executor.query_to_df(qry, post_queries=['COMMIT'])
                
                p_vars['standard']['sfd_monitor_dss_scenarios'] = str(dss_jobs_df['time_start'].max()) 
        except Exception as e:
            errors.append({
                'type': 'dss_scenario_runs',
                'exception': f'{traceback.format_exc()} | {qry}',
                'date': datetime.now()
            })  
            capture_exception(e) 

    # commits
    if dss_commit_df is not None:
        try:
            tm_stmp = str(int((datetime.now() - timedelta(days=30)).strftime('%s')) * 1000)
            if 'sfd_monitor_dss_commit' in p_vars['standard']:
                tm_stmp = p_vars["standard"]["sfd_monitor_dss_commit"]

            dss_commit_df = dss_commit_df.query(f'timestamp>{tm_stmp}')
                
            print(f'sending {len(dss_commit_df)} commits')

            if len(dss_commit_df) > 0:
                qry = f"INSERT INTO dataiku.dss_commits (\"account\", \"environment\", \"project_key\", \"commit_id\", \"author\", \"timestamp\") VALUES "

                for idx, row in dss_commit_df.iterrows():
                    proj = row['project_key']
                    commit = row['commit_id']
                    author = row['author']
                    timestamp = row['timestamp']

                    qry += f"('{ACCT_UN}', '{envt}', '{proj}', '{commit}', '{author}', {timestamp}),"

                qry = qry[0:-1]

                executor = SQLExecutor2(connection=SFD_CONN_NAME)
                executor.query_to_df(qry, post_queries=['COMMIT'])

                p_vars['standard']['sfd_monitor_dss_commit'] = str(dss_commit_df['timestamp'].max())
        except Exception as e:
            errors.append({
                'type': 'dss_commits',
                'exception': traceback.format_exc(),
                'date': datetime.now()
            })
            capture_exception(e) 

    # snowflake account
    if sending_snowflake:
        qry = ''
        try:
            tm_stmp = datetime.now() - timedelta(days=30)
            if 'sfd_snowflake_warehouse_metering' in p_vars['standard']:
                tm_stmp = p_vars["standard"]["sfd_snowflake_warehouse_metering"]
            
            ok_sf = True
            # only sending snowflake every hour
            if 'sfd_snowflake_warehouse_last_sync' in p_vars['standard']:                
                last_sync = datetime.strptime(p_vars['standard']['sfd_snowflake_warehouse_last_sync'][0:19], "%Y-%m-%d %H:%M:%S")
                sec_since_send = (datetime.now() - last_sync).seconds 

                ok_sf = sec_since_send >= 3600
            
            if ok_sf:            
                snowflake_warehouse_metering_df = snowflake_warehouse_meterings[0].get_dataframe()
                print(f'sending Snowflake data since it\'s been {sec_since_send}s')   
                p_vars['standard']['sfd_snowflake_warehouse_last_sync'] = str(datetime.now())

                # only non-null rows
                snowflake_warehouse_metering_df = snowflake_warehouse_metering_df.query(f'START_TIME>"{tm_stmp}+00:00" & END_TIME == END_TIME')

                if len(snowflake_warehouse_metering_df) > 0:
                    qry = f"INSERT INTO dataiku.snowflake_wh_usage (\"account\",\"start_time\",\"end_time\",\"warehouse_name\",\"credits_used\") VALUES "

                    for idx, row in snowflake_warehouse_metering_df.iterrows():
                        st = row['START_TIME']
                        et = row['END_TIME']
                        wh = row['WAREHOUSE_NAME']
                        credits = row['CREDITS_USED']

                        qry += f"('{ACCT_UN}','{st}','{et}','{wh}',{credits})," 

                    qry = qry[0:-1]

                    # print(qry)

                    executor = SQLExecutor2(connection=SFD_CONN_NAME)
                    executor.query_to_df(qry, post_queries=['COMMIT'])

                    p_vars['standard']['sfd_snowflake_warehouse_metering'] = str(snowflake_warehouse_metering_df['START_TIME'].max())

            
        except Exception as e:
            errors.append({
                'type': 'snowflake_warehouse_usage',
                'exception': f'{traceback.format_exc()} | {qry}',
                'date': datetime.now()
            })  
            capture_exception(e) 

insert_records(vals, vals_str, errors, dss_jobs_df, dss_commit_df, dss_scenarios_df, snowflake_warehouse_metering_df, proj)

# set any variable changes
client.get_default_project().set_variables(p_vars)

# Write recipe outputs
if error_output_ds != None:
    error_output_ds.write_with_schema(pd.DataFrame.from_dict(errors))
