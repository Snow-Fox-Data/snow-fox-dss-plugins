import dataiku
from dataiku.customrecipe import *
from dataiku import SQLExecutor2
import pandas as pd, numpy as np
import re

from mapping_utils import *

# Source
source_dataset = get_input_names_for_role('source_dataset')
source_datasets = [dataiku.Dataset(name) for name in source_dataset]

# Output
output_dataset = get_output_names_for_role('output_dataset')
output_datasets = [dataiku.Dataset(name) for name in output_dataset]

# Config
cfg = get_recipe_config()

space_replace = cfg['space_replace']
start_char_replace = cfg['start_char_replace']
special_char_replace = cfg['special_char_replace']
dollar_char_replace = cfg['dollar_char_replace']
to_upper = cfg['to_upper']

source_ds = source_datasets[0]
output_ds = output_datasets[0]

# map from Field => Remap Name
sql = do_map_simple(source_ds, to_upper, space_replace, special_char_replace, dollar_char_replace, start_char_replace)

executor = SQLExecutor2(dataset=source_ds)
executor.exec_recipe_fragment(output_ds, query = sql,overwrite_output_schema=True)
