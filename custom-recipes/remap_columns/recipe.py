import dataiku
from dataiku.customrecipe import *
from dataiku import SQLExecutor2
import pandas as pd, numpy as np
import re

from remap_columns import do_map
from remap_columns import add_description

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

# map from Field => Remap Name
sql, comments = do_map(source_ds, output_ds, map_df, table_name, table_field, desc_field, source_field, target_field)
executor = SQLExecutor2(dataset=source_ds)
executor.exec_recipe_fragment(output_ds, query = sql,overwrite_output_schema=True)

# after writing the output dataset, update the descriptions if a description is provided
add_description(desc_field, output_ds, comments)