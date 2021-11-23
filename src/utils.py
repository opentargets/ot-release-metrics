import base64
import pandas as pd

def get_table_download_link_csv(df):
    csv = df.to_csv().encode()
    b64 = base64.b64encode(csv).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="ot_metrics_data.csv" target="_blank">Download data</a>'
    return href

def add_delta(
    df: pd.DataFrame,
    metric: str,
    previous_run: str,
    latest_run: str):
    df[f"Δ in number of {metric}"] = (
        df[f"Nr of {metric} in {latest_run.split('-')[0]}"]
        .sub(
            df[f"Nr of {metric} in {previous_run.split('-')[0]}"],
            axis=0, fill_value=0))

def compare_entity(
    df: pd.DataFrame,
    entity_name: str,
    latest_run: str,
    previous_run: str
) -> pd.DataFrame:

    if entity_name in ['diseases', 'drugs', 'targets']:
        add_delta(df, entity_name, previous_run, latest_run)

    if entity_name == 'evidence':
        add_delta(df, "evidence strings", previous_run, latest_run)
        add_delta(df, "invalid evidence strings", previous_run, latest_run)
        add_delta(df, "evidence strings dropped due to duplication", previous_run, latest_run)
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
                'Δ in number of evidence strings dropped due to unresolved disease'
            ]
        )

    if entity_name == 'associations':
        add_delta(df, "direct associations", previous_run, latest_run)
        add_delta(df, "indirect associations", previous_run, latest_run)
        df.loc['Total'] = df.sum()
        df = df.filter(
            items=[
                f'Nr of indirect associations in {latest_run}',
                'Δ in number of indirect associations',
                f'Nr of direct associations in {latest_run}',
                'Δ in number of direct associations'
            ]
        )
    
    return df