# import the classes for accessing DSS objects from the recipe
import dataiku
from datetime import datetime
# Import the helpers for custom recipes
from dataiku.customrecipe import get_input_names_for_role
from dataiku.customrecipe import get_output_names_for_role
from dataiku.customrecipe import get_recipe_config

cfg = get_recipe_config()
model_id = cfg['model_id']
analysis_id = cfg['analysis_id']
mltask_id = cfg['mltask_id']
build_dataset = cfg['build_dataset']
build_recursive = cfg['build_recursive']

source_dataset = get_input_names_for_role('input_A_role')
model_in_flow = [dataiku.Model(name) for name in source_dataset][0]

# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
client = dataiku.api_client()
p = client.get_default_project()

# previously deployed model
# model_in_flow = dataiku.Model(model_id)
existing_versions = model_in_flow.list_versions()
# sm.delete_versions(versions_to_delete)

# here, we're getting a reference to the training task
mltask = p.get_ml_task(analysis_id, mltask_id)
ids = mltask.get_trained_models_ids()

ds = p.get_dataset(build_dataset)
logs = []

# iterate all the models in the training session (task)
for trained_model_id in ids:
    details = mltask.get_trained_model_details(trained_model_id)
    train_info = details.get_train_info()
    algorithm = details.get_modeling_settings()["algorithm"]
    auc = details.get_performance_metrics()["auc"]

    meta = details.get_user_meta()
    if meta['starred']:
        # print("Algorithm=%s AUC=%s" % (algorithm, auc))

        # deploys a new model to the flow
        # ret = mltask.deploy_to_flow(trained_model_id, "my_model", "sensordata_raw_prepared_joined_prepared")

        # or... redeploys to an existing node if the node doesn't already contain this version
        # finding existing version based on start / end times... should be a better way
        already_exists = list(filter(lambda version: version['snippet']['trainInfo']['startTime'] == train_info['startTime'] 
                                     and version['snippet']['trainInfo']['endTime'] == train_info['endTime'], existing_versions))
        log = { "date": datetime.now()}
        name = meta['name']
        if len(already_exists) == 0:
            mltask.redeploy_to_flow(trained_model_id, saved_model_id=model_in_flow.get_id(), activate=True)
            log['action'] = f'deploying {name} to flow'

            print(f'deploying {trained_model_id}')
        else:
            print(f'{trained_model_id} already deployed, re-activating')

            log['action'] = f'activating {name}'
            model_in_flow.activate_version(already_exists[0]['versionId'])

        tp = 'RECURSIVE_BUILD'
        if not build_recursive:
            tp = 'NON_RECURSIVE_FORCED_BUILD'
            
        ds.build(tp)
        logs.append(log)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Write recipe outputs
output_dataset = get_output_names_for_role('main_output')
output_datasets = [dataiku.Dataset(name) for name in output_dataset]

output_datasets[0].write_with_schema(pd.DataFrame.from_dict(logs))