import logging
import os

import gcsfs
import pandas as pd
import plotly.express as px
import streamlit as st

def show_table(
    name:str, latest_run: str,
    df: pd.DataFrame,
    yellow_bound: float, red_bound: float) -> None:
    """Displays the dataframe as a table in the Streamlit app."""
    st.header(f'{name.capitalize()} related metrics')
    try:
        st.table(
            df.fillna(0).astype(int)
            .style.apply(
                highlight_cell,
                entity=name, latest_run=latest_run,
                yellow_bound=yellow_bound, red_bound=red_bound,
                axis=1)
        )
    except Exception:
        # TODO: since the disease/target/drug tables have diff col names due to a bug, the styling is not applied
        st.table(df)

def highlight_cell(
    row: pd.Series, 
    entity: str, latest_run: str, 
    yellow_bound: float, red_bound: float) -> list[str]:
    """Highlights the cell in red if the count relative to the total number of evidence is higher than a set threshold.
    
    Args:
        row: row of the dataframe
        entity: name of the entity the metrics refer to
        latest_run: latest runId to indicate which column is useful to extract the total number
    
    Returns:
        An array of strings with the css property to pass the Pandas Styler and set the background color.
    """
    background = []
    total_count_name = f'Nr of indirect {entity} in {latest_run}' if entity == 'associations' else f'Nr of {entity} in {latest_run}'
    total_count = row[total_count_name]
    for cell in row:
        ratio = abs(cell / total_count)
        color = 'background-color: blank'
        if ratio >= red_bound and ratio != 1:
            color = 'background-color: red'
        elif ratio >= yellow_bound and ratio != 1:
            color = 'background-color: yellow'
        background.append(color)
    return background

@st.cache
def load_data(data_folder: str) -> pd.DataFrame:
    """This function reads all csv files from a provided location and returns as a concatenated pandas dataframe"""

    # Get list of csv files in a Google bucket:
    if data_folder.startswith('gs://'):
        csv_files = [
            f'gs://{x}'
            for x in gcsfs.GCSFileSystem().ls(data_folder)
            if x.endswith('csv')
        ]

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

def show_total_between_runs_for_entity(df: pd.DataFrame, entity_name: str, latest_run: str, previous_run: str) -> pd.DataFrame:
    """It takes a dataframe with metrics and returns a dataframe showing the total count between releasesfor a given entity.

    Args:
        df (pd.DataFrame): the dataframe with all metrics
        entity_name (str): the name of the entity you want to compare.
        latest_run (str): the name of the latest run
        previous_run (str): the name of the previous run

    Returns:
        pd.DataFrame: A dataframe with two columns (one per run) and one row with the total number of entities in each run.
    """
    total_count_col = f'{entity_name}TotalCount'
    return (
        df.query(
            'runId == @previous_run & variable == @total_count_col or runId == @latest_run & variable == @total_count_col'
        )
        [['runId', 'value']]
        # Prettify column names
        .assign(runId=lambda df: f"Nr of {entity_name} in " + df['runId'])
        # Transpose dataframe to have the runIds as columns
        .set_index('runId')
        .T
    )



def compare_entity(df: pd.DataFrame, entity_name: str, latest_run: str, previous_run: str) -> pd.DataFrame:
    """
    It takes a dataframe with metrics and returns a dataframe with the difference between the latest and previous run.

    Args:
      df (pd.DataFrame): the dataframe with all metrics
      entity_name (str): the name of the entity you want to compare.
      latest_run (str): the name of the latest run
      previous_run (str): the name of the previous run

    Returns:
      A dataframe with the number of entities in the latest run and the difference between the latest
    and previous run.
    """
    if entity_name in {'diseases', 'targets', 'drugs'}:
        print(df.columns)
        return show_total_between_runs_for_entity(
            df, entity_name, latest_run, previous_run
        )

    elif entity_name == 'associations':
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
        add_delta(df, "evidence", previous_run, latest_run)
        add_delta(df, "invalid evidence", previous_run, latest_run)
        add_delta(df, "evidence dropped due to duplication", previous_run, latest_run)

        add_delta(df, "evidence dropped due to null score", previous_run, latest_run)

        add_delta(df, "evidence dropped due to unresolved target", previous_run, latest_run)

        add_delta(df, "evidence dropped due to unresolved disease", previous_run, latest_run)

        df.loc['Total'] = df.sum()
        df = df.filter(
            items=[
                f'Nr of evidence in {latest_run}',
                'Δ in number of evidence',
                'Δ in number of invalid evidence',
                'Δ in number of evidence dropped due to duplication',
                'Δ in number of evidence dropped due to null score',
                'Δ in number of evidence dropped due to unresolved target',
                'Δ in number of evidence dropped due to unresolved disease',
            ]
        )

    return df
