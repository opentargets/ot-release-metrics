from google.cloud import bigquery
import great_expectations as gx
from typing import Any

PROJECT_ID = "open-targets-eu-dev"
DB_ID = "platform_dev"

def list_db_tables(dataset_id: str = DB_ID):
    """List all tables in the specified dataset.
    
    Args:
        dataset_id (str): The ID of the dataset to list tables for.
        
    Returns:
        A list of table IDs.
    """
    client = bigquery.Client(project=PROJECT_ID)
    dataset_ref = f"{PROJECT_ID}.{dataset_id}"
    
    tables = client.list_tables(dataset_ref)
    return [table.table_id for table in tables]


def get_bigquery_datasource(source_name: str = "my_bigquery"):
    """Create a BigQuery datasource in Great Expectations that connects to the Open Targets BigQuery database.
    
    Args:
        source_name (str): The name of the datasource to create.
        
    Returns:
        The created datasource.
    """
    context = gx.get_context()
    connection_string = f"bigquery://{PROJECT_ID}/{DB_ID}?credentials_path=Open Targets EU Dev.json"  # TODO: Resolver
    context.data_sources.add_or_update_sql(
        name=source_name, connection_string=connection_string
    )
    return context.data_sources.get(source_name)

def create_table_assets(data_source: Any, tables_list: list[str], project_id: str = PROJECT_ID, db_id: str = DB_ID):
    """Create Great Expectations assets for all tables in the Open Targets BigQuery database.
    
    Args:
        data_source (Any): The GE data source instance to create assets for.
        tables_list (list[str]): A list of table IDs to create assets for.
        project_id (str): The ID of the project containing the database.
        db_id (str): The ID of the database containing the tables.
    """
    for table_id in tables_list:
        try:
            asset = data_source.add_table_asset(
                name=table_id,
                table_name=f"{project_id}.{db_id}.{table_id}"
            )
            asset.add_batch_definition_whole_table(
                name=f"{table_id}_full_table"
            )  # full_table_batch_definition
            print(f"Successfully created asset for: {table_id}")
            
        except Exception as e:
            print(f"Error creating asset for {table_id}: {e}")

def add_expectations_for_table(asset_name: str, expectations: list[Any]):
    """Adds a suite and a validation definition containing the set of validations to run on a specific table.

        Suite defines the collection of expectations to run on a dataset.
        Validation definition ties data (through a batch definition) to a expectations suite.

    Args:
        asset_name (str): The name of the asset to add expectations for.
        expectations (list[Any]): A list of expectations to add to the suite.

    Returns:
        The created validation definition.
    """
    context = gx.get_context()

    # Add expectations to suite
    try:
        expectation_suite = context.suites.get(name=f"{asset_name}_expectations")
    except Exception:
        expectation_suite = context.suites.add(
            gx.ExpectationSuite(name=f"{asset_name}_expectations")
        )
    for expectation in expectations:
      expectation_suite.add_expectation(expectation)

    # Create validation definition
    batch_definition = (
        context.data_sources.get("my_bigquery")
        .get_asset(asset_name)
        .get_batch_definition(f"{asset_name}_full_table")
    )
    validation_definition = gx.ValidationDefinition(
      data=batch_definition, suite=expectation_suite, name=f"{asset_name}_validation"
    )
    try:
        validation_definition = context.validation_definitions.add(validation_definition)
    except Exception:
        print("Validation definition already exists. Deleting and recreating...")
        context.validation_definitions.delete(f"{asset_name}_validation")
        validation_definition = context.validation_definitions.add(validation_definition)
    return validation_definition


def run_validation(asset_name: str):
    """Run a checkpoint, which runs a set of validation definitions on a batch.
    
    Args:
        asset_name (str): The name of the asset to run validations on.
        
    Returns:
        The results of the checkpoint run.
    """
    context = gx.get_context()
    validation_definition = context.validation_definitions.get(f"{asset_name}_validation")

    try:
        checkpoint = context.checkpoints.get(f"{asset_name}_checkpoint")
    except Exception:
        checkpoint = context.checkpoints.add(
          gx.checkpoint.checkpoint.Checkpoint(
              name=f"{asset_name}_checkpoint", validation_definitions=[validation_definition]
          )
        )
    return checkpoint.run()