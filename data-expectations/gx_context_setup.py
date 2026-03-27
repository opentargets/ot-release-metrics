"""Set up of the great expectations context."""

import great_expectations as gx
from utils import list_db_tables, get_bigquery_datasource, create_table_assets

PROJECT_ID = "open-targets-eu-dev"
DB_ID = "platform_dev"

context = gx.data_context.FileDataContext._create(project_root_dir=".")

# Add or get data source
data_source = get_bigquery_datasource()

# Add assets
tables_to_add = [table_id for table_id in list_db_tables() if table_id not in ["ot_release"]]
create_table_assets(data_source, tables_to_add)
