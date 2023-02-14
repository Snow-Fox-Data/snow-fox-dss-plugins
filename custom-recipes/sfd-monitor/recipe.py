import dataiku
from dataiku.customrecipe import *
import pandas as pd
import numpy as np
from dataiku import pandasutils as pdu
from datetime import datetime, timezone, timedelta
import snowflake.connector
import time
import json
import os
import psutil

# Output
output_dataset = get_output_names_for_role('output_dataset')
output_datasets = [dataiku.Dataset(name) for name in output_dataset]
output_ds = output_datasets[0]

# Config
cfg = get_recipe_config()
send_jobs = cfg['send_jobs']

client = dataiku.api_client()
p_vars = client.get_default_project().get_variables()

ACCT_PW = p_vars['standard']['sfd_monitor_pw']
ACCT_UN = p_vars['standard']['sfd_monitor_un']
METRICS_TO_CHECK = p_vars['standard']['sfd_monitor_metrics']

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
dss_version = json.load(open(os.path.join(
    os.environ["DIP_HOME"], "dss-version.json")))["product_version"]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
vals_str = {
    'dss_version': dss_version
}

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# # System Stats

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Calling psutil.cpu_precent()
vals = {}
errors = []
try:
    vcpu = psutil.cpu_percent(interval=2)

    # Getting % usage of virtual_memory ( 3rd field)
    vmem = psutil.virtual_memory()

    vals['cpu_util_pct']: vcpu
    vals['ram_used_pct']: vmem.percent
    vals['ram_used_gb']: vmem.used/1000000000
    vals['ram_free_gb']: vmem.free/1000000000

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
        'exception': str(e)
    })

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE


# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ## Metrics

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
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
            'exception': str(e),
            'date': datetime.now()
        })

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ## Users

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
dss_users = client.list_users()

user_list = []
# Grab list of users where they have active web socket sessions
for user in dss_users:
    if user['activeWebSocketSesssions'] != 0:
        user_list.append(user['displayName'])
print(user_list)

vals['dss_user_count'] = len(user_list)
print(f'sending: {vals}')
print(f'sending: {vals_str}')
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
ts = time.time()
utc_offset = int((datetime.fromtimestamp(ts) -
                  datetime.utcfromtimestamp(ts)).total_seconds() / 60 / 60)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
dt_string = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

try:
    qry = f"INSERT INTO SNOWFOX_MONITOR.SFD.TS_DATA (\"account\", \"datetime\", \"key\", \"value_num\", \"value_str\", \"utc_offset\") VALUES "

    for key in vals:
        qry += f"('{ACCT_UN}', TO_TIMESTAMP_NTZ('{dt_string}'), '{key}', {vals[key]}, NULL, {utc_offset}),"

    for key in vals_str:
        qry += f"('{ACCT_UN}', TO_TIMESTAMP_NTZ('{dt_string}'), '{key}', NULL, '{vals_str[key]}', {utc_offset}),"

    qry = qry[0:-1]
except Exception as e:
    errors.append({
        'type': 'sql_val_gen',
        'exception': str(e),
        'date': datetime.now()
    })

ctx = snowflake.connector.connect(
    user=ACCT_UN,
    password=ACCT_PW,
    account='oh20501.us-east-1',
    warehouse="COMPUTE_WH",
    schema="SNOWFOX_MONITOR.SFD"
)

cs = ctx.cursor()
try:
    cs.execute(qry)
except Exception as e:
    errors.append({
        'type': 'sql',
        'exception': str(e),
        'date': datetime.now()
    })
finally:
    cs.close()

ctx.close()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# jobs
if send_jobs == 'yes':
    projects = []
    try:
        plist = client.list_project_keys()
        for p in plist:
            projects.append(client.get_project(p))

        last_job_time = int(
            (datetime.now() - timedelta(days=1)).strftime('%s')) * 1000
        if 'sfd_monitor_last_job_time' in p_vars['standard']:
            last_job_time = p_vars['standard']['sfd_monitor_last_job_time']

        sql_str = f"INSERT INTO SNOWFOX_MONITOR.SFD.DSS_JOBS (\"account\", \"project\", \"job_id\", \"recipe\", \"recipe_engine\", \"started\", \"ended\", \"total_seconds\") VALUES "

        for project in projects:
            jobs = project.list_jobs()
            #     running_jobs = [job for job in jobs if job['stableState'] == False]
            new_jobs = [
                job for job in jobs if job['startTime'] > last_job_time]

            latest_job = last_job_time
            for j in new_jobs:
                print(f'sending job: {j["def"]["id"]}')
                recipe = "NULL"
                recipe_type = "NULL"
                if 'recipe' in j['def']:
                    recipe = "'" + j['def']['recipe'] + "'"
                    recipe_dss = project.get_recipe(j['def']['recipe'])
                    status = recipe_dss.get_status()
                    recipe_type = "'" + \
                        status.get_selected_engine_details()['type'] + "'"

                st = datetime.fromtimestamp(
                    j['startTime']/1000).strftime("%Y-%m-%d %H:%M:%S")

                if j['startTime'] > latest_job:
                    latest_job = j['startTime']

                et = 'NULL'
                total_seconds = 'NULL'
                if 'endTime' in j:
                    total_seconds = (j['endTime'] - j['startTime']) / 1000
                    et = datetime.fromtimestamp(
                        j['endTime']/1000).strftime("%Y-%m-%d %H:%M:%S")

                sql_str += f"('{ACCT_UN}', '{j['def']['projectKey']}', '{j['def']['id']}', {recipe}, {recipe_type}, TO_TIMESTAMP_NTZ('{st}'), TO_TIMESTAMP_NTZ('{et}'), {total_seconds}),"

            p_vars['standard']['sfd_monitor_last_job_time'] = latest_job
            client.get_default_project().set_variables(p_vars)

        sql_str = sql_str[0:-1]
    except Exception as e:
        errors.append({
            'type': 'sql_proj_gen',
            'exception': str(e),
            'date': datetime.now()
        })

    # print(sql_str)
    ctx = snowflake.connector.connect(
        user=ACCT_UN,
        password=ACCT_PW,
        account='oh20501.us-east-1',
        warehouse="COMPUTE_WH",
        schema="SNOWFOX_MONITOR.SFD"
    )

    cs = ctx.cursor()
    try:
        cs.execute(sql_str)
    except Exception as e:
        errors.append({
            'type': 'sql',
            'exception': str(e),
            'date': datetime.now()
        })
    finally:
        cs.close()

    ctx.close()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Write recipe outputs
output_ds.write_with_schema(pd.DataFrame.from_dict(errors))
