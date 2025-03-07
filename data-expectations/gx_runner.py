"""Entry point to run the GX validation."""

import great_expectations as gx
from utils import run_validation

context = gx.get_context()

assets_list = context.data_sources.get("my_bigquery").get_asset_names()
for asset_name in assets_list:
    try:
        run_validation(asset_name=asset_name)
    except Exception as e:
        print(f"Validation failed for asset '{asset_name}'. Error: {e}")