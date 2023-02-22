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

# Output
output_dataset = get_output_names_for_role('output_dataset')
output_datasets = [dataiku.Dataset(name) for name in output_dataset]
output_ds = output_datasets[0]

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

# Config
cfg = get_recipe_config()
client = dataiku.api_client()
p_vars = client.get_default_project().get_variables()

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

def collect_server_stats(vals, errors):
    try:
        vcpu = psutil.cpu_percent(interval=2)

        # Getting % usage of virtual_memory ( 3rd field)
        vmem = psutil.virtual_memory()

        vals['cpu_util_pct'] = vcpu
        vals['ram_used_pct'] = vmem.percent
        vals['ram_used_gb'] = vmem.used/1000000000
        vals['ram_free_gb'] = vmem.free/1000000000

        # disks
        disks = psutil.disk_partitions(all=False)

        for disk in disks:
            usage = psutil.disk_usage(disks[0].mountpoint)

            d_name = disk.mountpoint.replace('/', '_')
            if d_name == '_':
                d_name = '_root'
            vals[f'disk{d_name}_used_gb'] = usage.used/1000000000
            vals[f'disk{d_name}_free_gb'] = usage.free/1000000000
            vals[f'disk{d_name}_used_pct'] = usage.percent

    except Exception as e:
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
            errors.append({
                'type': 'metric_string',
                'exception': f'{metric_to_check}: {traceback.format_exc()}',
                'date': datetime.now()
            })

def collect_user_project_data(vals, errors):
    try:
        dss_users = client.list_users()

        connected_user_ct = 0
        enabled_user_ct = 0

        # Grab list of users where they have active web socket sessions
        for user in dss_users:
            if user['activeWebSocketSesssions'] != 0:
                connected_user_ct += 1
            if user['enabled']:
                enabled_user_ct += 1

        vals['dss_user_connected_count'] = connected_user_ct
        vals['dss_user_enabled_count'] = enabled_user_ct
        vals['dss_project_count'] = len(client.list_project_keys())

    except Exception as e:
            errors.append({
                'type': 'user_project',
                'exception': traceback.format_exc(),
                'date': datetime.now()
            })

collect_server_stats(vals, errors)
collect_metrics(vals, vals_str, errors)
collect_user_project_data(vals, errors)

print(f'sending: {vals}')
print(f'sending: {vals_str}')

def insert_records(vals, vals_str, errors, dss_jobs_df, dss_commit_df):
    ts = time.time()
    utc_offset = int((datetime.fromtimestamp(ts) -
                    datetime.utcfromtimestamp(ts)).total_seconds() / 60 / 60)
    dt_string = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
        qry = f"INSERT INTO dataiku.ts_data (\"account\", \"datetime\", \"key\", \"value_num\", \"value_str\", \"utc_offset\") VALUES "

        for key in vals:
            qry += f"('{ACCT_UN}', '{dt_string}', '{key}', {vals[key]}, NULL, {utc_offset}),"

        for key in vals_str:
            qry += f"('{ACCT_UN}', '{dt_string}', '{key}', NULL, '{vals_str[key]}', {utc_offset}),"

        qry = qry[0:-1]

        executor = SQLExecutor2(connection=SFD_CONN_NAME)
        executor.query_to_df(qry, post_queries=['COMMIT'])
    except Exception as e:
        errors.append({
            'type': 'sql_val_gen',
            'exception': str(e),
            'date': datetime.now()
        })

    # jobs
    if dss_jobs_df is not None:
        try:
            tm_stmp = str(int((datetime.now() - timedelta(days=30)).strftime('%s')) * 1000)
            if 'sfd_monitor_dss_jobs' in p_vars['standard']:
                tm_stmp = p_vars["standard"]["sfd_monitor_dss_jobs"]

            dss_jobs_df = dss_jobs_df.query(f'timestamp>{tm_stmp}')

            qry = f"INSERT INTO dataiku.dss_jobs (\"account\","
            for c in dss_jobs_df.columns:
                qry += f"\"{c}\","
            
            qry = qry[0:-1]
            qry += ') VALUES '

            for idx, row in dss_jobs_df.iterrows():
                qry += f"('{ACCT_UN}',"

                for c in dss_jobs_df.columns:
                    qry += f"'{row[c]}',"

                qry = qry[0:-1]
                qry += '),'
            
            qry = qry[0:-1]

            executor = SQLExecutor2(connection=SFD_CONN_NAME)
            executor.query_to_df(qry, post_queries=['COMMIT'])
            
            p_vars['standard']['sfd_monitor_dss_jobs'] = str(dss_jobs_df['timestamp'].max()) 
        except Exception as e:
            errors.append({
                'type': 'dss_jobs',
                'exception': traceback.format_exc(),
                'date': datetime.now()
            })   

    # commits
    if dss_commit_df is not None:
        try:
            tm_stmp = str(int((datetime.now() - timedelta(days=30)).strftime('%s')) * 1000)
            if 'sfd_monitor_dss_commit' in p_vars['standard']:
                tm_stmp = p_vars["standard"]["sfd_monitor_dss_commit"]

            dss_commit_df = dss_commit_df.query(f'timestamp>{tm_stmp}')
                
            print(f'sending {len(dss_commit_df)} commits')

            if len(dss_commit_df) > 0:
                qry = f"INSERT INTO dataiku.dss_commits (\"account\", \"project_key\", \"commit_id\", \"author\", \"timestamp\") VALUES "

                for idx, row in dss_commit_df.iterrows():
                    proj = row['project_key']
                    commit = row['commit_id']
                    author = row['author']
                    timestamp = row['timestamp']

                    qry += f"('{ACCT_UN}', '{proj}', '{commit}', '{author}', {timestamp}),"

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

insert_records(vals, vals_str, errors, dss_jobs_df, dss_commit_df)

# set any variable changes
client.get_default_project().set_variables(p_vars)

# Write recipe outputs
output_ds.write_with_schema(pd.DataFrame.from_dict(errors))
