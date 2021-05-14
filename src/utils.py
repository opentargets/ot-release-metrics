import base64
import pandas as pd

def get_table_download_link_csv(df):
    csv = df.to_csv().encode()
    b64 = base64.b64encode(csv).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="ot_metrics_data.csv" target="_blank">Download data</a>'
    return href

def compare_evidence(
    evidence: pd.DataFrame,
    latest_run: str,
    previous_run: str
) -> pd.DataFrame:
    evidence["Δ in number of evidence strings"] = evidence[f"Nr of evidence strings in {latest_run.split('-')[0]}"].sub(evidence[f"Nr of evidence strings in {previous_run.split('-')[0]}"], axis=0, fill_value=0)
    evidence["Δ in number of invalid evidence strings"] = evidence[f"Nr of invalid evidence strings in {latest_run.split('-')[0]}"].sub(evidence[f"Nr of invalid evidence strings in {previous_run.split('-')[0]}"], axis=0, fill_value=0)
    evidence["Δ in number of evidence strings dropped due to duplication"] = evidence[f"Nr of evidence strings dropped due to duplication in {latest_run.split('-')[0]}"].sub(evidence[f"Nr of evidence strings dropped due to duplication in {previous_run.split('-')[0]}"], axis=0, fill_value=0)
    evidence["Δ in number of evidence strings dropped due to unresolved target"] = evidence[f"Nr of evidence strings dropped due to unresolved target in {latest_run.split('-')[0]}"].sub(evidence[f"Nr of evidence strings dropped due to unresolved target in {previous_run.split('-')[0]}"], axis=0, fill_value=0)
    evidence["Δ in number of evidence strings dropped due to unresolved disease"] = evidence[f"Nr of evidence strings dropped due to unresolved disease in {latest_run.split('-')[0]}"].sub(evidence[f"Nr of evidence strings dropped due to unresolved disease in {previous_run.split('-')[0]}"], axis=0, fill_value=0)
    evidence = evidence.iloc[:-1] # Delete row with NaN
    evidence.loc['Total'] = evidence.sum()
    return evidence[evidence.columns[-5:]]

def compare_association(
    association: pd.DataFrame,
    latest_run: str,
    previous_run: str
) -> pd.DataFrame:
    association["Δ in number of direct associations"] = association[f"Nr of direct associations in {latest_run.split('-')[0]}"].sub(association[f"Nr of direct associations in {previous_run.split('-')[0]}"], axis=0, fill_value=0)
    association["Δ in number of indirect associations"] = association[f"Nr of indirect associations in {latest_run.split('-')[0]}"].sub(association[f"Nr of indirect associations in {previous_run.split('-')[0]}"], axis=0, fill_value=0)
    association.loc['Total'] = association.sum()
    return association[association.columns[-2:]]

def compare_disease(
    disease: pd.DataFrame,
    latest_run: str,
    previous_run: str
) -> pd.DataFrame:
    disease["Δ in number of diseases"] = disease[f"Nr of diseases in {latest_run.split('-')[0]}"].sub(disease[f"Nr of diseases in {previous_run.split('-')[0]}"], axis=0, fill_value=0)
    return disease

def compare_drug(
    drug: pd.DataFrame,
    latest_run: str,
    previous_run: str
) -> pd.DataFrame:
    drug["Δ in number of drugs"] = drug[f"Nr of drugs in {latest_run.split('-')[0]}"].sub(drug[f"Nr of drugs in {previous_run.split('-')[0]}"], axis=0, fill_value=0)
    return drug
