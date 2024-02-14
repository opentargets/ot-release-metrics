from functools import reduce

import hydra
from omegaconf import DictConfig
import pandas as pd
from PIL import Image
import streamlit as st

from src.metric_visualisation.utils import (
    extract_primary_run_id_list,
    show_table,
    load_data,
    plot_enrichment,
    compare_entity,
    select_and_mask_data_to_compare,
    select_and_mask_data_to_explore,
)


@hydra.main(version_base=None, config_path="config", config_name="config")
def main(cfg: DictConfig):
    # App UI
    st.set_page_config(
        page_title="Open Targets Data Metrics",
        page_icon=Image.open("src/assets/img/favicon.png"),
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("A sneak peek at the stats in the Open Targets Platform.")
    st.markdown("##")
    st.markdown(
        "This application is a dashboard to display the metrics of the different data releases in Open Targets."
    )

    # Add a "Refresh" button.
    refresh_btn = st.button("Refresh list of runs")
    if refresh_btn:
        st.cache_data.clear()
        st.rerun()

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
    data = load_data(cfg.metric_calculation.data_repositories.metrics_root)
    all_runs = list(data.runId.unique())
    all_releases = extract_primary_run_id_list(all_runs)

    if page == "Explore metrics":
        data, selected_run = select_and_mask_data_to_explore(all_releases, all_runs, data)
        if selected_run:
            st.markdown("## Key metrics")
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric(
                label="Evidence",
                value=int(
                    data.query('variable == "evidenceTotalCount"')["value"].values[0]
                ),
            )
            # Print post ETL metrics only when available
            if "pre" not in selected_run:
                col2.metric(
                    label="Associations",
                    value=int(
                        data.query('variable == "associationsIndirectTotalCount"')[
                            "value"
                        ].values[0]
                    ),
                )
                col3.metric(
                    label="Targets",
                    value=int(
                        data.query('variable == "targetsTotalCount"')["value"].values[0]
                    ),
                )
                col4.metric(
                    label="Diseases",
                    value=int(
                        data.query('variable == "diseasesTotalCount"')["value"].values[
                            0
                        ]
                    ),
                )
                col5.metric(
                    label="Drugs",
                    value=int(
                        data.query('variable == "drugsTotalCount"')["value"].values[0]
                    ),
                )

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
                    select_fields = st.multiselect(
                        "You can also filter by your field of interest:",
                        data.field.unique(),
                    )
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
                        .dropna(axis="columns", how="any")
                    )
                    output.columns = output.columns.get_level_values(1)
                except ValueError as e:
                    print(e)
                    st.write(
                        "Please, indicate a specific pipeline run to group and explore the data."
                    )
            else:
                output = data.copy()

            # Display table
            st.dataframe(output.style.format(precision=2))
            st.download_button(
                label="Download data as CSV",
                data=output.to_csv().encode("utf-8"),
                file_name=f"metrics-{selected_run}.csv",
                mime="text/csv",
            )

    if page == "Compare metrics":
        data, selected_runs = select_and_mask_data_to_compare(all_releases, all_runs, data)

        if selected_runs:
            latest_run, previous_run = selected_runs
            # Compute variables
            # EVIDENCE
            old_evidence_count = data.query(
                'runId == @previous_run & variable == "evidenceCountByDatasource"'
            )[["value", "datasourceId"]].rename(
                {"value": f"Nr of evidence in {previous_run}"}, axis=1
            )
            new_evidence_count = data.query(
                'runId == @latest_run & variable == "evidenceCountByDatasource"'
            )[["value", "datasourceId"]].rename(
                {"value": f"Nr of evidence in {latest_run}"}, axis=1
            )
            old_evidence_invalid = data.query(
                'runId == @previous_run & variable == "evidenceInvalidCountByDatasource"'
            )[["value", "datasourceId"]].rename(
                {"value": f"Nr of invalid evidence in {previous_run}"}, axis=1
            )
            new_evidence_invalid = data.query(
                'runId == @latest_run & variable == "evidenceInvalidCountByDatasource"'
            )[["value", "datasourceId"]].rename(
                {"value": f"Nr of invalid evidence in {latest_run}"}, axis=1
            )
            old_evidence_duplicates = data.query(
                'runId == @previous_run & variable == "evidenceDuplicateCountByDatasource"'
            )[["value", "datasourceId"]].rename(
                {
                    "value": f"Nr of evidence dropped due to duplication in {previous_run}"
                },
                axis=1,
            )
            new_evidence_duplicates = data.query(
                'runId == @latest_run & variable == "evidenceDuplicateCountByDatasource"'
            )[["value", "datasourceId"]].rename(
                {"value": f"Nr of evidence dropped due to duplication in {latest_run}"},
                axis=1,
            )
            old_evidence_null_score = data.query(
                'runId == @previous_run & variable == "evidenceNullifiedScoreCountByDatasource"'
            )[["value", "datasourceId"]].rename(
                {
                    "value": f"Nr of evidence dropped due to null score in {previous_run}"
                },
                axis=1,
            )
            new_evidence_null_score = data.query(
                'runId == @latest_run & variable == "evidenceNullifiedScoreCountByDatasource"'
            )[["value", "datasourceId"]].rename(
                {"value": f"Nr of evidence dropped due to null score in {latest_run}"},
                axis=1,
            )
            old_evidence_unresolved_target = data.query(
                'runId == @previous_run & variable == "evidenceUnresolvedTargetCountByDatasource"'
            )[["value", "datasourceId"]].rename(
                {
                    "value": f"Nr of evidence dropped due to unresolved target in {previous_run}"
                },
                axis=1,
            )
            new_evidence_unresolved_target = data.query(
                'runId == @latest_run & variable == "evidenceUnresolvedTargetCountByDatasource"'
            )[["value", "datasourceId"]].rename(
                {
                    "value": f"Nr of evidence dropped due to unresolved target in {latest_run}"
                },
                axis=1,
            )
            old_evidence_unresolved_disease = data.query(
                'runId == @previous_run & variable == "evidenceUnresolvedDiseaseCountByDatasource"'
            )[["value", "datasourceId"]].rename(
                {
                    "value": f"Nr of evidence dropped due to unresolved disease in {previous_run}"
                },
                axis=1,
            )
            new_evidence_unresolved_disease = data.query(
                'runId == @latest_run & variable == "evidenceUnresolvedDiseaseCountByDatasource"'
            )[["value", "datasourceId"]].rename(
                {
                    "value": f"Nr of evidence dropped due to unresolved disease in {latest_run}"
                },
                axis=1,
            )

            # ASSOCIATION
            old_indirect_association = data.query(
                'runId == @previous_run & variable == "associationsIndirectByDatasource"'
            )[["value", "datasourceId"]].rename(
                {"value": f"Nr of indirect associations in {previous_run}"}, axis=1
            )
            new_indirect_association = data.query(
                'runId == @latest_run & variable == "associationsIndirectByDatasource"'
            )[["value", "datasourceId"]].rename(
                {"value": f"Nr of indirect associations in {latest_run}"}, axis=1
            )
            old_direct_association = data.query(
                'runId == @previous_run & variable == "associationsDirectByDatasource"'
            )[["value", "datasourceId"]].rename(
                {"value": f"Nr of direct associations in {previous_run}"}, axis=1
            )
            new_direct_association = data.query(
                'runId == @latest_run & variable == "associationsDirectByDatasource"'
            )[["value", "datasourceId"]].rename(
                {"value": f"Nr of direct associations in {latest_run}"}, axis=1
            )

            # Aggregate metrics
            evidence_datasets = [
                old_evidence_count,
                new_evidence_count,
                old_evidence_invalid,
                new_evidence_invalid,
                old_evidence_duplicates,
                new_evidence_duplicates,
                old_evidence_null_score,
                new_evidence_null_score,
                old_evidence_unresolved_target,
                new_evidence_unresolved_target,
                old_evidence_unresolved_disease,
                new_evidence_unresolved_disease,
            ]
            evidence = (
                reduce(
                    lambda x, y: pd.merge(x, y, on="datasourceId", how="outer"),
                    evidence_datasets,
                )
                .set_index("datasourceId")
                .fillna(0)
            )
            association_datasets = [
                old_indirect_association,
                new_indirect_association,
                old_direct_association,
                new_direct_association,
            ]
            association = reduce(
                lambda x, y: pd.merge(x, y, on="datasourceId"), association_datasets
            ).set_index("datasourceId")

            # Compare datasets
            evidence_comparison = compare_entity(
                evidence, "evidence", latest_run, previous_run
            )
            association_comparison = compare_entity(
                association, "associations", latest_run, previous_run
            )
            disease_comparison = compare_entity(
                data, "diseases", latest_run, previous_run
            )
            target_comparison = compare_entity(
                data, "targets", latest_run, previous_run
            )
            drug_comparison = compare_entity(data, "drugs", latest_run, previous_run)

            # Display tables
            dfs = [
                ("evidence", evidence_comparison),
                ("associations", association_comparison),
                ("diseases", disease_comparison),
                ("targets", target_comparison),
                ("drugs", drug_comparison),
            ]
            for name, df in dfs:
                show_table(
                    name,
                    latest_run,
                    df,
                    cfg.metric_visualization.parameters.yellow_highlight_bound,
                    cfg.metric_visualization.parameters.red_highlight_bound,
                )

    if page == "Visualise metrics":
        data, selected_runs = select_and_mask_data_to_compare(all_releases, all_runs, data)
        if selected_runs:
            st.plotly_chart(plot_enrichment(data), use_container_width=True)

    st.markdown("###")
    st.image(Image.open("src/assets/img/logo.png"), width=150)


if __name__ == "__main__":
    main()
