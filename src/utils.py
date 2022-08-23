import logging
import os

import gcsfs
import pandas as pd
import plotly.express as px
from psutil import virtual_memory
from pyspark.sql import SparkSession, DataFrame
import streamlit as st


def initialize_spark_session():
    """
    It creates a SparkSession object with the driver and executor memory set to the value of the
    spark_mem_limit variable
    
    Returns:
      A SparkSession object
    """
    spark_mem_limit = detect_spark_memory_limit()
    return (
        SparkSession.builder.config("spark.driver.memory", f'{spark_mem_limit}G')
        .config("spark.executor.memory", f'{spark_mem_limit}G')
        .getOrCreate()
    )


def detect_spark_memory_limit():
    """Spark does not automatically use all available memory on a machine. When working on large datasets, this may
    cause Java heap space errors, even though there is plenty of RAM available. To fix this, we detect the total amount
    of physical memory and allow Spark to use (almost) all of it."""
    mem_gib = virtual_memory().total >> 30
    return int(mem_gib * 0.9)


def read_path_if_provided(spark, path: str):
    """
    Automatically detect the format of the input data and read it into the Spark dataframe. The supported formats
    are: a single TSV file; a single JSON file; a directory with JSON files; a directory with Parquet files.
    """
    # All datasets are optional.
    if path is None:
        return None

    # The provided path must exist and must be either a file or a directory.
    assert os.path.exists(path), f'The provided path {path} does not exist.'
    assert os.path.isdir(path) or os.path.isfile(path), f'The provided path {path} is neither a file or a directory.'

    # Case 1: We are provided with a single file.
    if os.path.isfile(path):
        if path.endswith('.tsv'):
            return spark.getOrCreate.read.csv(path, sep='\t', header=True)
        elif path.endswith(('.json', '.json.gz', '.jsonl', '.jsonl.gz')):
            return spark.read.json(path)
        else:
            raise AssertionError(f'The format of the provided file {path} is not supported.')

    # Case 2: We are provided with a directory. Let's peek inside to see what it contains.
    all_files = [os.path.join(dp, filename) for dp, dn, filenames in os.walk(path) for filename in filenames]

    # It must be either exclusively JSON, or exclusively Parquet.
    json_files = [fn for fn in all_files if fn.endswith(('.json', '.json.gz', '.jsonl', '.jsonl.gz'))]
    parquet_files = [fn for fn in all_files if fn.endswith('.parquet')]
    assert not (json_files and parquet_files), f'The provided directory {path} contains a mix of JSON and Parquet.'
    assert json_files or parquet_files, f'The provided directory {path} contains neither JSON nor Parquet.'

    # A directory with JSON files.
    if json_files:
        return spark.read.option('recursiveFileLookup', 'true').json(path)

    # A directory with Parquet files.
    if parquet_files:
        return spark.read.parquet(path)


def add_delta(df: pd.DataFrame, metric: str, previous_run: str, latest_run: str):
    """
    It takes a dataframe, a metric, and two run names, and adds a column to the dataframe that shows the
    difference in the number of that metric between the two runs

    Args:
      df (pd.DataFrame): the dataframe to add the delta column to
      metric (str): the metric we're interested in
      previous_run (str): the name of the previous run
      latest_run (str): the latest run of the pipeline
    """

    df[f"Δ in number of {metric}"] = df[f"Nr of {metric} in {latest_run.split('-')[0]}"].sub(
        df[f"Nr of {metric} in {previous_run.split('-')[0]}"], axis=0, fill_value=0
    )


def compare_entity(df: pd.DataFrame, entity_name: str, latest_run: str, previous_run: str) -> pd.DataFrame:
    """
    It takes a dataframe with metrics and returns a dataframe with the difference between the latest and previous run.

    Args:
      df (pd.DataFrame): the dataframe to be modified
      entity_name (str): the name of the entity you want to compare.
      latest_run (str): the name of the latest run
      previous_run (str): the name of the previous run

    Returns:
      A dataframe with the number of entities in the latest run and the difference between the latest
    and previous run.
    """
    if entity_name in {'diseases', 'drugs', 'targets'}:
        add_delta(df, entity_name, previous_run, latest_run)
    if entity_name == 'associations':
        add_delta(df, "direct associations", previous_run, latest_run)
        add_delta(df, "indirect associations", previous_run, latest_run)
        df.loc['Total'] = df.sum()
        df = df.filter(
            items=[
                f'Nr of indirect associations in {latest_run}',
                'Δ in number of indirect associations',
                f'Nr of direct associations in {latest_run}',
                'Δ in number of direct associations',
            ]
        )

    elif entity_name == 'evidence':
        add_delta(df, "evidence strings", previous_run, latest_run)
        add_delta(df, "invalid evidence strings", previous_run, latest_run)
        add_delta(df, "evidence strings dropped due to duplication", previous_run, latest_run)

        add_delta(df, "evidence strings dropped due to null score", previous_run, latest_run)

        add_delta(df, "evidence strings dropped due to unresolved target", previous_run, latest_run)

        add_delta(df, "evidence strings dropped due to unresolved disease", previous_run, latest_run)

        df.loc['Total'] = df.sum()
        df = df.filter(
            items=[
                f'Nr of evidence strings in {latest_run}',
                'Δ in number of evidence strings',
                'Δ in number of invalid evidence strings',
                'Δ in number of evidence strings dropped due to duplication',
                'Δ in number of evidence strings dropped due to unresolved target',
                'Δ in number of evidence strings dropped due to unresolved disease',
            ]
        )

    return df


def plot_enrichment(data: pd.DataFrame):
    """Creates scatter plot that displays the different OR/AUC values per runId across data sources."""

    # Filter data per variables of interest
    masks_variable = (data["variable"] == "associationsIndirectByDatasourceAUC") | (
        data["variable"] == "associationsIndirectByDatasourceOR"
    )
    data = data[masks_variable].drop(['field', 'count'], axis=1)

    # Convert df from long to wide so that the variables (rows) become features (columns)
    data_unstacked = data.set_index(['datasourceId', 'runId', 'variable']).value.unstack().reset_index()

    # Design plot
    enrichment_plot = px.scatter(
        data_unstacked,
        x='associationsIndirectByDatasourceOR',
        y='associationsIndirectByDatasourceAUC',
        log_x=True,
        log_y=False,
        color='runId',
        hover_data=['datasourceId'],
        template='plotly_white',
        title='Enrichment of indirect associations between releases across data sources',
        labels={'associationsIndirectByDatasourceOR': 'OR', 'associationsIndirectByDatasourceAUC': 'AUC'},
    )
    enrichment_plot.add_hline(y=0.5, line_dash="dash", opacity=0.2)
    enrichment_plot.add_vline(x=1, line_dash="dash", opacity=0.2)
    enrichment_plot.update_yaxes(range=(0.35, 1), constrain='domain')

    return enrichment_plot


@st.cache
def load_data(data_folder: str) -> pd.DataFrame:
    """This function reads all csv files from a provided location and returns as a concatenated pandas dataframe"""

    # Get list of csv files in a Google bucket:
    if data_folder.startswith('gs://'):
        csv_files = ['gs://' + x for x in gcsfs.GCSFileSystem().ls(data_folder) if x.endswith('csv')]

    # Get list of csv files in a local folder:
    else:
        csv_files = [os.path.join(data_folder, x) for x in os.listdir(data_folder) if x.endswith('csv')]

    dataset_count = len(csv_files)
    logging.info(f'Number of csv files found in the data folder: {dataset_count}')

    # Reading csv files as pandas dataframes:
    while True:
        data = pd.concat(
            [pd.read_csv(x, sep=',', dtype={'runId': 'string'}) for x in csv_files], ignore_index=True
        ).fillna({'value': 0})
        logging.info(f'Number of rows in the dataframe: {len(data)}')
        logging.info(f'Number of datasets: {len(data.runId.unique())}')

        if len(data.runId.unique()) == dataset_count:
            logging.info('All datasets loaded successfully.')
            break

    return data
