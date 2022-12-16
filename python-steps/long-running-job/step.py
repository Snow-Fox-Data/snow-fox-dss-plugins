# This file is the code for the plugin Python step long-running-job

import os, json
from dataiku.customstep import *
import dataiku
import time

# the plugin's resource folder path (string)
# resource_folder = get_step_resource()

# settings at the plugin level (set by plugin administrators in the Plugins section)
# plugin_config = get_plugin_config()

# settings at the step instance level (set by the user creating a scenario step)
step_config = get_step_config()
client = dataiku.api_client()

run_type = step_config['run_type']

projects = []
if run_type == '0':
    p = client.get_default_project()
    projects.append(p)
else:
    plist = client.list_project_keys()
    for p in plist:
        projects.append(client.get_project(p))

for project in projects:
    jobs = project.list_jobs()
    running_jobs = [job for job in jobs if job['stableState'] == False]

    epoch = time.time() * 1000
    if len(running_jobs) > 0:
        for j in running_jobs:
            duration_seconds = (epoch - j['startTime']) / 1000
            msg = f'job {j["def"]["id"]} has been running for {duration_seconds}s'
            if duration_seconds > step_config['max_seconds']:
                raise Exception(msg)

            print(msg)
       