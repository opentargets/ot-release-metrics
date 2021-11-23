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

def compare_evidence(
    evidence: pd.DataFrame,
    latest_run: str,
    previous_run: str
) -> pd.DataFrame:
    add_delta(evidence, "evidence strings", previous_run, latest_run)
    add_delta(evidence, "invalid evidence strings", previous_run, latest_run)
    add_delta(evidence, "evidence strings dropped due to duplication", previous_run, latest_run)
    add_delta(evidence, "evidence strings dropped due to unresolved target", previous_run, latest_run)
    add_delta(evidence, "evidence strings dropped due to unresolved disease", previous_run, latest_run)
    evidence = evidence.iloc[:-1]  # Delete row with NaN
    evidence.loc['Total'] = evidence.sum()
    evidence = evidence.filter(
        items=[
            f'Nr of evidence strings in {latest_run}',
            'Δ in number of evidence strings',
            'Δ in number of invalid evidence strings',
            'Δ in number of evidence strings dropped due to duplication',
            'Δ in number of evidence strings dropped due to unresolved target',
            'Δ in number of evidence strings dropped due to unresolved disease'
        ]
    )
    return evidence

def compare_association(
    association: pd.DataFrame,
    latest_run: str,
    previous_run: str
) -> pd.DataFrame:
    add_delta(association, "direct associations", previous_run, latest_run)
    add_delta(association, "indirect associations", previous_run, latest_run)
    association.loc['Total'] = association.sum()
    association = association.filter(
        items=[
            f'Nr of indirect associations in {latest_run}',
            'Δ in number of indirect associations',
            f'Nr of direct associations in {latest_run}',
            'Δ in number of direct associations'
        ]
    )
    return association

def compare_entity(
    dataset: pd.DataFrame,
    entity_name: str,
    latest_run: str,
    previous_run: str
) -> pd.DataFrame:
    add_delta(dataset, entity_name, previous_run, latest_run)
    return dataset