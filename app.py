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

    # Display metrics
    select_pivot = st.checkbox("See pivot table.")
    if select_pivot:
        output = pd.pivot_table(data, values="count", index=["variable", "field"], columns="datasourceId")
    else:
        output = data.copy()
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
            .rename({"count" : "Nr of evidence strings in the last release"}, axis=1)
        )
        new_evidence_count = (data
            .query('runId == @latest_run & variable == "evidenceCountByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : "Nr of evidence strings in the latest release"}, axis=1)
        )
        new_evidence_invalid = (data
            .query('runId == @latest_run & variable == "evidenceInvalidCountByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : "Nr of invalid evidence strings"}, axis=1)
        )
        new_evidence_duplicates = (data
            .query('runId == @latest_run & variable == "evidenceDuplicateCountByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : "Nr of evidence strings dropped due to duplication"}, axis=1)
        )
        new_evidence_unresolved_target = (data
            .query('runId == @latest_run & variable == "evidenceUnresolvedTargetCountByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : "Nr of evidence strings dropped due to unresolved target"}, axis=1)
        )
        new_evidence_unresolved_disease = (data
            .query('runId == @latest_run & variable == "evidenceUnresolvedDiseaseCountByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : "Nr of evidence strings dropped due to unresolved disease"}, axis=1)
        )
        
        # ASSOCIATION
        old_indirect_association = (data
            .query('runId == @previous_run & variable == "associationsIndirectByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : "Nr of indirect associations in the last release"}, axis=1)
        )
        new_indirect_association = (data
            .query('runId == @latest_run & variable == "associationsIndirectByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : "Nr of indirect associations in the latest release"}, axis=1)
        )
        old_direct_association = (data
            .query('runId == @previous_run & variable == "associationsDirectByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : "Nr of direct associations in the last release"}, axis=1)
        )
        new_indirect_association = (data
            .query('runId == @latest_run & variable == "associationsDirectByDatasource"')[["count", "datasourceId"]]
            .rename({"count" : "Nr of direct associations in the latest release"}, axis=1)
        )

        # DISEASES
        old_diseases_count = (data
            .query('runId == @previous_run & variable == "diseasesTotalCount"')[["count", "datasourceId"]]
            .rename({"count" : "Nr of diseases in the last release"}, axis=1)
        )
        new_diseases_count = (data
            .query('runId == @latest_run & variable == "diseasesTotalCount"')[["count", "datasourceId"]]
            .rename({"count" : "Nr of diseases in the latest release"}, axis=1)
        )

        # Aggregate metrics
        evidence_datasets = [
            old_evidence_count, new_evidence_count,
            new_evidence_invalid, new_evidence_duplicates,
            new_evidence_unresolved_target, new_evidence_unresolved_disease
        ]
        evidence = reduce(lambda x, y: pd.merge(x, y, on = "datasourceId"), evidence_datasets).set_index("datasourceId")
        
        association_datasets = [
            old_indirect_association, new_indirect_association,
            old_direct_association, new_indirect_association,
        ]
        association = reduce(lambda x, y: pd.merge(x, y, on = "datasourceId"), association_datasets).set_index("datasourceId")

        # Display tables
        st.header("Evidence related metrics:")
        st.table(evidence)
        st.header("Associations related metrics:")
        st.table(association)

st.image(Image.open("img/OT logo.png"), width = 150)