from functools import reduce
import glob

import streamlit as st
import pandas as pd
from PIL import Image

from src.utils import *

# App UI
st.set_page_config(
    page_title="Open Targets Data Metrics",
    page_icon=Image.open("img/favicon.png"),
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("A sneak peek at the stats in the Open Targets Platform.")
st.markdown('##')
st.markdown("This application is a dashboard to display the metrics of the different data releases in Open Targets.")

st.sidebar.header("What do you want to do?")
page = st.sidebar.radio(
    "Choose an option",
    ("Explore metrics", "Compare metrics", "Visualise metrics"),
    index=1,
    help=(
        "Explore metrics allows you to visualise and filter the metrics of the selected datasets. Compare metrics "
        "allows you to compare the main metrics between two releases."
    ),
)

# Load data
files = glob.glob("data/*.csv")
dfs = [pd.read_csv(f, dtype={'runId': 'string'}) for f in files]
data = pd.concat(dfs, ignore_index=True).fillna({'value': 0})

if page == "Explore metrics":
    # Select a dataset to explore
    runs = sorted(list(data.runId.unique()), reverse=True)
    runs.insert(0, "All")
    select_run = st.sidebar.selectbox("Select a pipeline run:", runs)

    # Apply select_run mask
    if select_run != "All":
        mask_run = data["runId"] == select_run
        data = data[mask_run]

        st.markdown("## Key metrics")
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric(label='Evidence', value=int(data.query('variable == "evidenceTotalCount"')['value'].values[0]))
        col2.metric(
            label='Associations',
            value=int(data.query('variable == "associationsIndirectTotalCount"')['value'].values[0]),
        )
        col3.metric(label='Targets', value=int(data.query('variable == "targetsTotalCount"')['value'].values[0]))
        col4.metric(label='Diseases', value=int(data.query('variable == "diseasesTotalCount"')['value'].values[0]))
        col5.metric(label='Drugs', value=int(data.query('variable == "drugsTotalCount"')['value'].values[0]))

        # Refine the query
        st.markdown("## Filter the data")
        select_variables = st.multiselect(
            "Filter by your variables of interest to refine your analysis. You can select as many as you want:",
            data.variable.unique(),
        )
        if select_variables:
            mask_variable_selection = data["variable"].isin(select_variables)
            data = data[mask_variable_selection]
            if any("Field" in variable for variable in select_variables):
                select_fields = st.multiselect("You can also filter by your field of interest:", data.field.unique())
                mask_field = data["field"].isin(select_fields)
                data = data[mask_field] if select_fields else data.copy()

        # Generate pivot table
        select_pivot = st.checkbox("See pivot table.")
        if select_pivot:
            try:
                # Custom pivot table to avoid loss of data when field == NaN
                output = (
                    data.set_index(["variable", "field", "datasourceId"])
                    .unstack("datasourceId", fill_value=0)
                    .drop("runId", axis=1)
                    # There is a current bug where the data source columns are duplicated with NaN values
                    # These will be temporarily removed to improve the metrics exploration in the UI
                    .dropna(axis='columns', how='any')
                )
                output.columns = output.columns.get_level_values(1)
            except ValueError:
                st.write("Please, indicate a specific pipeline run to group and explore the data.")
        else:
            output = data.copy()

        # Display table
        st.dataframe(output.style.set_precision(2))
        st.download_button(
            label="Download data as CSV",
            data=output.to_csv().encode('utf-8'),
            file_name=f'metrics-{select_run}.csv',
            mime='text/csv',
        )

if page == "Compare metrics":
    # Select two datasets to compare
    st.sidebar.header("What do you want to compare?")
    select_runs = st.sidebar.multiselect(
        "Select two datasets:",
        sorted(data.runId.unique(), reverse=True),
        help="First indicate the run with which you wish to make the comparison.",
    )

    # Apply masks
    if len(select_runs) == 2:
        previous_run = select_runs[0]
        latest_run = select_runs[1]
        masks_run = (data["runId"] == previous_run) | (data["runId"] == latest_run)
        data = data[masks_run]

        # Compute variables
        # EVIDENCE
        old_evidence_count = data.query('runId == @previous_run & variable == "evidenceCountByDatasource"')[
            ["value", "datasourceId"]
        ].rename({"value": f"Nr of evidence strings in {previous_run.split('-')[0]}"}, axis=1)
        new_evidence_count = data.query('runId == @latest_run & variable == "evidenceCountByDatasource"')[
            ["value", "datasourceId"]
        ].rename({"value": f"Nr of evidence strings in {latest_run.split('-')[0]}"}, axis=1)
        old_evidence_invalid = data.query('runId == @previous_run & variable == "evidenceInvalidCountByDatasource"')[
            ["value", "datasourceId"]
        ].rename({"value": f"Nr of invalid evidence strings in {previous_run.split('-')[0]}"}, axis=1)
        new_evidence_invalid = data.query('runId == @latest_run & variable == "evidenceInvalidCountByDatasource"')[
            ["value", "datasourceId"]
        ].rename({"value": f"Nr of invalid evidence strings in {latest_run.split('-')[0]}"}, axis=1)
        old_evidence_duplicates = data.query(
            'runId == @previous_run & variable == "evidenceDuplicateCountByDatasource"'
        )[["value", "datasourceId"]].rename(
            {"value": f"Nr of evidence strings dropped due to duplication in {previous_run.split('-')[0]}"}, axis=1
        )
        new_evidence_duplicates = data.query('runId == @latest_run & variable == "evidenceDuplicateCountByDatasource"')[
            ["value", "datasourceId"]
        ].rename({"value": f"Nr of evidence strings dropped due to duplication in {latest_run.split('-')[0]}"}, axis=1)
        old_evidence_unresolved_target = data.query(
            'runId == @previous_run & variable == "evidenceUnresolvedTargetCountByDatasource"'
        )[["value", "datasourceId"]].rename(
            {"value": f"Nr of evidence strings dropped due to unresolved target in {previous_run.split('-')[0]}"},
            axis=1,
        )
        new_evidence_unresolved_target = data.query(
            'runId == @latest_run & variable == "evidenceUnresolvedTargetCountByDatasource"'
        )[["value", "datasourceId"]].rename(
            {"value": f"Nr of evidence strings dropped due to unresolved target in {latest_run.split('-')[0]}"}, axis=1
        )
        old_evidence_unresolved_disease = data.query(
            'runId == @previous_run & variable == "evidenceUnresolvedDiseaseCountByDatasource"'
        )[["value", "datasourceId"]].rename(
            {"value": f"Nr of evidence strings dropped due to unresolved disease in {previous_run.split('-')[0]}"},
            axis=1,
        )
        new_evidence_unresolved_disease = data.query(
            'runId == @latest_run & variable == "evidenceUnresolvedDiseaseCountByDatasource"'
        )[["value", "datasourceId"]].rename(
            {"value": f"Nr of evidence strings dropped due to unresolved disease in {latest_run.split('-')[0]}"}, axis=1
        )

        # ASSOCIATION
        old_indirect_association = data.query(
            'runId == @previous_run & variable == "associationsIndirectByDatasource"'
        )[["value", "datasourceId"]].rename(
            {"value": f"Nr of indirect associations in {previous_run.split('-')[0]}"}, axis=1
        )
        new_indirect_association = data.query('runId == @latest_run & variable == "associationsIndirectByDatasource"')[
            ["value", "datasourceId"]
        ].rename({"value": f"Nr of indirect associations in {latest_run.split('-')[0]}"}, axis=1)
        old_direct_association = data.query('runId == @previous_run & variable == "associationsDirectByDatasource"')[
            ["value", "datasourceId"]
        ].rename({"value": f"Nr of direct associations in {previous_run.split('-')[0]}"}, axis=1)
        new_direct_association = data.query('runId == @latest_run & variable == "associationsDirectByDatasource"')[
            ["value", "datasourceId"]
        ].rename({"value": f"Nr of direct associations in {latest_run.split('-')[0]}"}, axis=1)

        # DISEASES
        old_diseases_count = data.query('runId == @previous_run & variable == "diseasesTotalCount"')[["value"]].rename(
            {"value": f"Nr of diseases in {previous_run.split('-')[0]}"}, axis=1
        )
        new_diseases_count = data.query('runId == @latest_run & variable == "diseasesTotalCount"')[["value"]].rename(
            {"value": f"Nr of diseases in {latest_run.split('-')[0]}"}, axis=1
        )

        # DRUGS
        old_drugs_count = data.query('runId == @previous_run & variable == "drugsTotalCount"')["value"].rename(
            {"value": f"Nr of drugs in {previous_run.split('-')[0]}"}, axis=1
        )
        new_drugs_count = data.query('runId == @latest_run & variable == "drugsTotalCount"')["value"].rename(
            {"value": f"Nr of drugs in {latest_run.split('-')[0]}"}, axis=1
        )

        # TARGETS
        old_targets_count = data.query('runId == @previous_run & variable == "targetsTotalCount"')[["value"]].rename(
            {"value": f"Nr of targets in {previous_run.split('-')[0]}"}, axis=1
        )
        new_targets_count = data.query('runId == @latest_run & variable == "targetsTotalCount"')[["value"]].rename(
            {"value": f"Nr of targets in {latest_run.split('-')[0]}"}, axis=1
        )

        # Aggregate metrics
        evidence_datasets = [
            old_evidence_count,
            new_evidence_count,
            old_evidence_invalid,
            new_evidence_invalid,
            old_evidence_duplicates,
            new_evidence_duplicates,
            old_evidence_unresolved_target,
            new_evidence_unresolved_target,
            old_evidence_unresolved_disease,
            new_evidence_unresolved_disease,
        ]
        evidence = (
            reduce(lambda x, y: pd.merge(x, y, on="datasourceId", how="outer"), evidence_datasets)
            .set_index("datasourceId")
            .fillna(0)
        )
        association_datasets = [
            old_indirect_association,
            new_indirect_association,
            old_direct_association,
            new_direct_association,
        ]
        association = reduce(lambda x, y: pd.merge(x, y, on="datasourceId"), association_datasets).set_index(
            "datasourceId"
        )

        disease_datasets = [old_diseases_count, new_diseases_count]
        disease = pd.concat(disease_datasets, axis=0, ignore_index=True).T.rename(columns={0: "value"})

        target_datasets = [old_targets_count, new_targets_count]
        target = pd.concat(target_datasets, axis=0, ignore_index=True).T.rename(columns={0: "value"})

        drug_datasets = [old_drugs_count, new_drugs_count]
        drug = pd.concat(drug_datasets, axis=0, ignore_index=True).T

        # Compare datasets
        evidence_comparison = compare_entity(evidence, 'evidence', latest_run, previous_run)
        association_comparison = compare_entity(association, 'associations', latest_run, previous_run)
        # TODO: All comparisons are failing, executing the except block. compare_entity must be fixed
        try:
            disease_comparison = compare_entity(disease, 'diseases', latest_run, previous_run)
        except KeyError:
            disease_comparison = disease.copy()
        try:
            target_comparison = compare_entity(target, 'targets', latest_run, previous_run)
        except KeyError:
            target_comparison = target.copy()
        try:
            drug_comparison = compare_entity(drug, 'drugs', latest_run, previous_run)
        except KeyError:
            drug_comparison = drug.copy().reindex([f"Nr of drugs in {previous_run}", f"Nr of drugs in {latest_run}"])

        # Display tables
        st.header("Evidence related metrics:")
        st.table(evidence_comparison.astype(int))
        st.header("Associations related metrics:")
        st.table(association_comparison.astype(int))
        st.header("Disease related metrics:")
        st.table(disease_comparison)
        st.header("Target related metrics:")
        st.table(target_comparison)
        st.header("Drug related metrics:")
        st.table(drug_comparison)

if page == "Visualise metrics":
    # Select two datasets to compare
    st.sidebar.header("What do you want to compare?")
    select_runs = st.sidebar.multiselect(
        "Select two datasets to see the level of enrichment per data source:", sorted(data.runId.unique(), reverse=True)
    )

    # Apply masks
    if len(select_runs) == 2:
        previous_run = select_runs[0]
        latest_run = select_runs[1]

        # Filter data per selected runIds and variables of interest
        masks_run = (data["runId"] == previous_run) | (data["runId"] == latest_run)
        data = data[masks_run]

        # Plot
        st.plotly_chart(plot_enrichment(data), use_container_width=True)

st.markdown('###')
st.image(Image.open("img/OT logo.png"), width=150)
