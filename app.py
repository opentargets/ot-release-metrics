import glob
from functools import reduce

import streamlit as st
import pandas as pd
from PIL import Image

from src.utils import *

# App UI
st.set_page_config(
    page_title="Open Targets Data Metrics",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("A sneak peek at the stats in the Open Targets Platform.")
st.markdown('##')
st.markdown("This application is a dashboard to display the metrics of the different data releases in Open Targets.")

st.sidebar.header("What do you want to do?")
page = st.sidebar.radio(
    "Choose an option",
    ("Explore metrics", "Compare metrics", "Statistics"),
    index=1,
    help="Explore metrics allows you to visualise and filter the metrics of the selected datasets. Compare metrics allows you to compare the main metrics between two releases."
)

# Load data
files = glob.glob("data/*.csv")
dfs = [pd.read_csv(f) for f in files]
data = pd.concat(dfs, ignore_index=True)

if page == "Explore metrics":
    # Select a dataset to explore
    runs = list(data.runId.unique())
    runs.insert(0, "All")
    select_run = st.sidebar.selectbox(
        "Select a pipeline run:",
        runs
    )

    # Apply select_run mask
    if select_run != "All":
        mask_run = data["runId"] == select_run
        data = data[mask_run]
    
    # Refine the query
    st.markdown("## Filter the data")
    select_variables = st.multiselect(
        "Filter by your variables of interest to refine your analysis. You can select as many as you want:",
        data.variable.unique()
    )
    if select_variables:
        mask_variable = data["variable"].isin(select_variables)
        data = data[mask_variable]
        if any("Field" in variable for variable in select_variables):
            select_fields = st.multiselect(
                "You can also filter by your field of interest:",
                data.field.unique()
            )
            mask_field = data["field"].isin(select_fields)
            data = data[mask_field] if select_fields else data.copy()

    # Generate pivot table
    select_pivot = st.checkbox("See pivot table.")
    if select_pivot:
        try:
            # Custom pivot table to avoid loss of data when field == NaN
            output = data.set_index(["variable", "field", "datasourceId"]).unstack("datasourceId", fill_value=0).drop("runId", axis=1)
            output.columns = output.columns.get_level_values(1)
        except ValueError:
            st.write("Please, indicate a specific pipeline run to group and explore the data.")
    else:
        output = data.copy()

    # Display table
    st.write(output)
    st.markdown(get_table_download_link_csv(output), unsafe_allow_html=True)

if page == "Compare metrics":
    # Select two datasets to compare
    st.sidebar.header("What do you want to compare?")
    select_runs = st.sidebar.multiselect(
        "Select two datasets:",
        data.runId.unique(),
        help="First indicate the run with which you wish to make the comparison."
    )

    # Apply masks
    if len(select_runs) >=2:
        previous_run = select_runs[0]
        latest_run = select_runs[1]
        masks_run = (data["runId"] == previous_run) | (data["runId"] == latest_run)
        data = data[masks_run]

        # Compute variables
        # EVIDENCE
        old_evidence_count = (data
            .query('runId == @previous_run & variable == "evidenceCountByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : f"Nr of evidence strings in {previous_run.split('-')[0]}"}, axis=1)
        )
        new_evidence_count = (data
            .query('runId == @latest_run & variable == "evidenceCountByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : f"Nr of evidence strings in {latest_run.split('-')[0]}"}, axis=1)
        )
        old_evidence_invalid = (data
            .query('runId == @previous_run & variable == "evidenceInvalidCountByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : f"Nr of invalid evidence strings in {previous_run.split('-')[0]}"}, axis=1)
        )
        new_evidence_invalid = (data
            .query('runId == @latest_run & variable == "evidenceInvalidCountByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : f"Nr of invalid evidence strings in {latest_run.split('-')[0]}"}, axis=1)
        )
        old_evidence_duplicates = (data
            .query('runId == @previous_run & variable == "evidenceDuplicateCountByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : f"Nr of evidence strings dropped due to duplication in {previous_run.split('-')[0]}"}, axis=1)
        )
        new_evidence_duplicates = (data
            .query('runId == @latest_run & variable == "evidenceDuplicateCountByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : f"Nr of evidence strings dropped due to duplication in {latest_run.split('-')[0]}"}, axis=1)
        )
        old_evidence_unresolved_target = (data
            .query('runId == @previous_run & variable == "evidenceUnresolvedTargetCountByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : f"Nr of evidence strings dropped due to unresolved target in {previous_run.split('-')[0]}"}, axis=1)
        )
        new_evidence_unresolved_target = (data
            .query('runId == @latest_run & variable == "evidenceUnresolvedTargetCountByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : f"Nr of evidence strings dropped due to unresolved target in {latest_run.split('-')[0]}"}, axis=1)
        )
        old_evidence_unresolved_disease = (data
            .query('runId == @previous_run & variable == "evidenceUnresolvedDiseaseCountByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : f"Nr of evidence strings dropped due to unresolved disease in {previous_run.split('-')[0]}"}, axis=1)
        )
        new_evidence_unresolved_disease = (data
            .query('runId == @latest_run & variable == "evidenceUnresolvedDiseaseCountByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : f"Nr of evidence strings dropped due to unresolved disease in {latest_run.split('-')[0]}"}, axis=1)
        )
        
        # ASSOCIATION
        old_indirect_association = (data
            .query('runId == @previous_run & variable == "associationsIndirectByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : f"Nr of indirect associations in {previous_run.split('-')[0]}"}, axis=1)
        )
        new_indirect_association = (data
            .query('runId == @latest_run & variable == "associationsIndirectByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : f"Nr of indirect associations in {latest_run.split('-')[0]}"}, axis=1)
        )
        old_direct_association = (data
            .query('runId == @previous_run & variable == "associationsDirectByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : f"Nr of direct associations in {previous_run.split('-')[0]}"}, axis=1)
        )
        new_direct_association = (data
            .query('runId == @latest_run & variable == "associationsDirectByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : f"Nr of direct associations in {latest_run.split('-')[0]}"}, axis=1)
        )

        # DISEASES
        old_diseases_count = (data
            .query('runId == @previous_run & variable == "diseasesTotalCount"')[["count"]]
            .rename({"count" : f"Nr of diseases in {previous_run.split('-')[0]}"}, axis=1)
        )
        new_diseases_count = (data
            .query('runId == @latest_run & variable == "diseasesTotalCount"')[["count"]]
            .rename({"count" : f"Nr of diseases in {latest_run.split('-')[0]}"}, axis=1)
        )

        # DRUGS
        old_drugs_count = (data
            .query('runId == @previous_run & variable == "drugsTotalCount"')["count"]
            .rename({"count" : f"Nr of drugs in {previous_run.split('-')[0]}"}, axis=1)
        )
        new_drugs_count = (data
            .query('runId == @latest_run & variable == "drugsTotalCount"')["count"]
            .rename({"count" : f"Nr of drugs in {latest_run.split('-')[0]}"}, axis=1)
        )

        # Aggregate metrics
        evidence_datasets = [
            old_evidence_count, new_evidence_count,
            old_evidence_invalid, new_evidence_invalid,
            old_evidence_duplicates, new_evidence_duplicates,
            old_evidence_unresolved_target, new_evidence_unresolved_target,
            old_evidence_unresolved_disease, new_evidence_unresolved_disease
        ]
        evidence = reduce(lambda x, y: pd.merge(x, y, on="datasourceId", how="outer"), evidence_datasets).set_index("datasourceId").fillna(0)

        association_datasets = [
            old_indirect_association, new_indirect_association,
            old_direct_association, new_direct_association,
        ]
        association = reduce(lambda x, y: pd.merge(x, y, on="datasourceId"), association_datasets).set_index("datasourceId")
        
        disease_datasets = [old_diseases_count, new_diseases_count]
        disease = pd.concat(disease_datasets, axis=0, ignore_index=True).T.rename(columns={0: "count"})
        
        drug_datasets = [old_drugs_count, new_drugs_count]
        drug = pd.concat(drug_datasets, axis=0, ignore_index=True).T

        # Compare datasets
        evidence_comparison = compare_evidence(evidence, latest_run, previous_run)
        association_comparison = compare_association(association, latest_run, previous_run)
        try:
            disease_comparison = compare_disease(disease, latest_run, previous_run)
        except KeyError:
            disease_comparison = disease.copy()
        try:
            drug_comparison = compare_drug(drug, latest_run, previous_run)
        except KeyError:
            drug_comparison = drug.copy()

        # Display tables
        st.header("Evidence related metrics:")
        st.table(evidence_comparison)
        st.header("Associations related metrics:")
        st.table(association_comparison)
        st.header("Disease related metrics:")
        st.table(disease_comparison)
        st.header("Drug related metrics: WIP")
        st.table(drug_comparison)

st.markdown('###')
st.image(Image.open("img/OT logo.png"), width = 150)