from functools import reduce

import pandas as pd
import streamlit as st
from PIL import Image
from src.utils import (
    compare_entity,
    extract_primary_run_id_list,
    get_asset_path,
    load_data_from_hf,
    plot_enrichment,
    select_and_mask_data_to_compare,
    select_and_mask_data_to_explore,
    show_table,
)

YELLOW_HIGHLIGHT_BOUND = 0.2
RED_HIGHLIGHT_BOUND = 0.5


def render_metric_rows(metrics: list[tuple[str, int]], per_row: int = 4) -> None:
    for i in range(0, len(metrics), per_row):
        row_metrics = metrics[i : i + per_row]
        cols = st.columns(len(row_metrics))
        for col, (label, value) in zip(cols, row_metrics):
            col.metric(label=label, value=value)


def get_metric_value(data: pd.DataFrame, *variables: str) -> int | None:
    for variable in variables:
        metric_values = data.loc[data['variable'] == variable, 'value']
        if metric_values.empty:
            continue

        value = metric_values.iloc[0]
        if pd.isna(value):
            continue

        try:
            return int(value)
        except (TypeError, ValueError):
            continue

    return None


def main():
    # App UI
    st.set_page_config(
        page_title='Open Targets Data Metrics',
        page_icon=Image.open(get_asset_path('src/assets/img/favicon.png')),
        layout='wide',
        initial_sidebar_state='expanded',
    )

    st.title('A sneak peek at the stats in the Open Targets Platform.')
    st.markdown('##')
    st.markdown(
        'This application is a dashboard to display the metrics of the different data releases in Open Targets.'
    )

    # Add a "Refresh" button.
    refresh_btn = st.button('Refresh list of runs')
    if refresh_btn:
        st.cache_data.clear()
        st.rerun()

    st.sidebar.header('What do you want to do?')
    page = st.sidebar.radio(
        'Choose an option',
        ('Explore metrics', 'Compare metrics', 'Visualise metrics'),
        index=1,
        help=(
            'Explore metrics allows you to visualise and filter the metrics of the selected datasets. Compare metrics '
            'allows you to compare the main metrics between two releases.'
        ),
    )
    data = load_data_from_hf()
    all_runs = list(data.runId.unique())
    all_releases = extract_primary_run_id_list(all_runs)

    if page == 'Explore metrics':
        data, selected_run = select_and_mask_data_to_explore(
            all_releases, all_runs, data
        )
        if selected_run:
            st.markdown('## Key metrics')
            metric_specs: list[tuple[str, tuple[str, ...]]] = [
                ('Evidence', ('evidenceTotalCount',)),
                ('Associations', ('associationsIndirectTotalCount',)),
                ('Targets', ('targetsTotalCount',)),
                ('Diseases', ('diseasesTotalCount', 'diseaseTotalCount')),
                ('Drugs', ('drugsTotalCount',)),
                ('Studies', ('studyTotalCount',)),
                ('Credible sets', ('credibleSetTotalCount',)),
                ('Variants', ('variantTotalCount',)),
            ]

            key_metrics: list[tuple[str, int]] = []
            for label, variables in metric_specs:
                value = get_metric_value(data, *variables)
                if value is not None:
                    key_metrics.append((label, value))

            if key_metrics:
                render_metric_rows(key_metrics, per_row=4)
            else:
                st.info(
                    'No key metrics are available for the selected run. '
                    'Try another run or refresh the list of runs.'
                )

            # Refine the query
            st.markdown('## Filter the data')
            select_variables = st.multiselect(
                'Filter by your variables of interest to refine your analysis. You can select as many as you want:',
                data.variable.unique(),
            )
            if select_variables:
                mask_variable_selection = data['variable'].isin(select_variables)
                data = data[mask_variable_selection]
                if any('Field' in variable for variable in select_variables):
                    select_fields = st.multiselect(
                        'You can also filter by your field of interest:',
                        data.field.unique(),
                    )
                    mask_field = data['field'].isin(select_fields)
                    data = data[mask_field] if select_fields else data.copy()

            # Generate pivot table
            select_pivot = st.checkbox('See pivot table.')
            if select_pivot:
                try:
                    # Custom pivot table to avoid loss of data when field == NaN
                    output = (
                        data.set_index(['variable', 'field', 'datasourceId'])
                        .unstack('datasourceId', fill_value=0)
                        .drop('runId', axis=1)
                        # There is a current bug where the data source columns are duplicated with NaN values
                        # These will be temporarily removed to improve the metrics exploration in the UI
                        .dropna(axis='columns', how='any')
                    )
                    output.columns = output.columns.get_level_values(1)
                except ValueError as e:
                    print(e)
                    st.write(
                        'Please, indicate a specific pipeline run to group and explore the data.'
                    )
            else:
                output = data.copy()

            # Display table
            st.dataframe(output.style.format(precision=2))
            st.download_button(
                label='Download data as CSV',
                data=output.to_csv().encode('utf-8'),
                file_name=f'metrics-{selected_run}.csv',
                mime='text/csv',
            )

    if page == 'Compare metrics':
        data, selected_runs = select_and_mask_data_to_compare(
            all_releases, all_runs, data
        )

        if selected_runs:
            latest_run, previous_run = selected_runs
            # Compute variables
            # EVIDENCE
            old_evidence_count = data.query(
                'runId == @previous_run & variable == "evidenceCountByDatasource"'
            )[['value', 'datasourceId']].rename(
                {'value': f'Nr of evidence in {previous_run}'}, axis=1
            )
            new_evidence_count = data.query(
                'runId == @latest_run & variable == "evidenceCountByDatasource"'
            )[['value', 'datasourceId']].rename(
                {'value': f'Nr of evidence in {latest_run}'}, axis=1
            )
            old_evidence_invalid = data.query(
                'runId == @previous_run & variable == "evidenceInvalidCountByDatasource"'
            )[['value', 'datasourceId']].rename(
                {'value': f'Nr of invalid evidence in {previous_run}'}, axis=1
            )
            new_evidence_invalid = data.query(
                'runId == @latest_run & variable == "evidenceInvalidCountByDatasource"'
            )[['value', 'datasourceId']].rename(
                {'value': f'Nr of invalid evidence in {latest_run}'}, axis=1
            )
            old_evidence_duplicates = data.query(
                'runId == @previous_run & variable == "evidenceDuplicateCountByDatasource"'
            )[['value', 'datasourceId']].rename(
                {
                    'value': f'Nr of evidence dropped due to duplication in {previous_run}'
                },
                axis=1,
            )
            new_evidence_duplicates = data.query(
                'runId == @latest_run & variable == "evidenceDuplicateCountByDatasource"'
            )[['value', 'datasourceId']].rename(
                {'value': f'Nr of evidence dropped due to duplication in {latest_run}'},
                axis=1,
            )
            old_evidence_null_score = data.query(
                'runId == @previous_run & variable == "evidenceNullifiedScoreCountByDatasource"'
            )[['value', 'datasourceId']].rename(
                {
                    'value': f'Nr of evidence dropped due to null score in {previous_run}'
                },
                axis=1,
            )
            new_evidence_null_score = data.query(
                'runId == @latest_run & variable == "evidenceNullifiedScoreCountByDatasource"'
            )[['value', 'datasourceId']].rename(
                {'value': f'Nr of evidence dropped due to null score in {latest_run}'},
                axis=1,
            )
            old_evidence_unresolved_target = data.query(
                'runId == @previous_run & variable == "evidenceUnresolvedTargetCountByDatasource"'
            )[['value', 'datasourceId']].rename(
                {
                    'value': f'Nr of evidence dropped due to unresolved target in {previous_run}'
                },
                axis=1,
            )
            new_evidence_unresolved_target = data.query(
                'runId == @latest_run & variable == "evidenceUnresolvedTargetCountByDatasource"'
            )[['value', 'datasourceId']].rename(
                {
                    'value': f'Nr of evidence dropped due to unresolved target in {latest_run}'
                },
                axis=1,
            )
            old_evidence_unresolved_disease = data.query(
                'runId == @previous_run & variable == "evidenceUnresolvedDiseaseCountByDatasource"'
            )[['value', 'datasourceId']].rename(
                {
                    'value': f'Nr of evidence dropped due to unresolved disease in {previous_run}'
                },
                axis=1,
            )
            new_evidence_unresolved_disease = data.query(
                'runId == @latest_run & variable == "evidenceUnresolvedDiseaseCountByDatasource"'
            )[['value', 'datasourceId']].rename(
                {
                    'value': f'Nr of evidence dropped due to unresolved disease in {latest_run}'
                },
                axis=1,
            )

            # ASSOCIATION
            old_indirect_association = data.query(
                'runId == @previous_run & variable == "associationsIndirectByDatasource"'
            )[['value', 'datasourceId']].rename(
                {'value': f'Nr of indirect associations in {previous_run}'}, axis=1
            )
            new_indirect_association = data.query(
                'runId == @latest_run & variable == "associationsIndirectByDatasource"'
            )[['value', 'datasourceId']].rename(
                {'value': f'Nr of indirect associations in {latest_run}'}, axis=1
            )
            old_direct_association = data.query(
                'runId == @previous_run & variable == "associationsDirectByDatasource"'
            )[['value', 'datasourceId']].rename(
                {'value': f'Nr of direct associations in {previous_run}'}, axis=1
            )
            new_direct_association = data.query(
                'runId == @latest_run & variable == "associationsDirectByDatasource"'
            )[['value', 'datasourceId']].rename(
                {'value': f'Nr of direct associations in {latest_run}'}, axis=1
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
                    lambda x, y: pd.merge(x, y, on='datasourceId', how='outer'),
                    evidence_datasets,
                )
                .set_index('datasourceId')
                .fillna(0)
            )
            association_datasets = [
                old_indirect_association,
                new_indirect_association,
                old_direct_association,
                new_direct_association,
            ]
            association = reduce(
                lambda x, y: pd.merge(x, y, on='datasourceId'), association_datasets
            ).set_index('datasourceId')

            # Compare datasets
            evidence_comparison = compare_entity(
                evidence, 'evidence', latest_run, previous_run
            )
            association_comparison = compare_entity(
                association, 'associations', latest_run, previous_run
            )
            disease_comparison = compare_entity(
                data, 'diseases', latest_run, previous_run
            )
            target_comparison = compare_entity(
                data, 'targets', latest_run, previous_run
            )
            drug_comparison = compare_entity(data, 'drugs', latest_run, previous_run)

            # Display tables
            dfs = [
                ('evidence', evidence_comparison),
                ('associations', association_comparison),
                ('diseases', disease_comparison),
                ('targets', target_comparison),
                ('drugs', drug_comparison),
            ]
            for name, df in dfs:
                show_table(
                    name,
                    latest_run,
                    df,
                    YELLOW_HIGHLIGHT_BOUND,
                    RED_HIGHLIGHT_BOUND,
                )

    if page == 'Visualise metrics':
        data, selected_runs = select_and_mask_data_to_compare(
            all_releases, all_runs, data
        )
        if selected_runs:
            st.plotly_chart(plot_enrichment(data), use_container_width=True)

    st.markdown('###')
    st.image(Image.open(get_asset_path('src/assets/img/logo.png')), width=150)


if __name__ == '__main__':
    main()
