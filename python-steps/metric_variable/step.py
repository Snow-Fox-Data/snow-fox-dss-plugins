import os, json
from dataiku.customstep import *
import dataiku
import time

def compute_metric(dataset_name, metric_id):
    client = dataiku.api_client()
    proj = client.get_default_project()
    
    dataset = proj.get_dataset(dataset_name)
    
    dataset.compute_metrics(
         metric_ids=[metric_id]
    )

def get_metric(dataset_name, metric_id):
    dataset = dataiku.Dataset(dataset_name)
    
    metrics = dataset.get_last_metric_values()
    metric = metrics.get_metric_by_id(metric_id)
    
    if 'lastValues' in metric:
        if len(metric['lastValues']) > 0:
            mv = metric['lastValues'][0]
            if 'INT' in mv['dataType']:
                return int(mv['value'])
            
            return mv['value']
    
    return ''

def set_pvar(key, val):
    client = dataiku.api_client()
    proj = client.get_default_project()
    vbls = proj.get_variables()
    
    vbls['standard'][key] = val
    proj.set_variables(vbls)
    
    
step_config = get_step_config()

ds = step_config['source_dataset']
metric = step_config['metric_name']


compute_metric(ds, metric)
mv = get_metric(ds, metric)

var_name = step_config['variable_name']
set_pvar(var_name, mv)