#!/usr/bin/env python3
"""Calculates the various quality metrics for evidence and the associated datasets.

For the post-pipeline run, the ${ETL_INPUT_ROOT} is gs://open-targets-data-releases/${RELEASE}/input/.
The ${ETL_PARQUET_OUTPUT_ROOT} is:
* gs://ot-snapshots/etl/outputs/${RELEASE}/parquet/ for the snapshots (releases in progress);
* gs://open-targets-data-releases/${RELEASE}/output/etl-parquet/ for the completed releases."""

import argparse
from functools import reduce
import logging
import logging.config
from typing import Iterable

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f
import pyspark.sql.types as t


def flatten(schema, prefix=None):
    """Required to flatten the schema."""
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, t.ArrayType):
            dtype = dtype.elementType
        if isinstance(dtype, t.StructType):
            fields += flatten(dtype, prefix=name)
        else:
            fields.append(name)
    return fields


def melt(
        df: DataFrame,
        id_vars: Iterable[str], value_vars: Iterable[str],
        var_name: str = 'variable', value_name: str = 'value') -> DataFrame:
    """Convert :class:`DataFrame` from wide to long format."""

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = f.array(*(
        f.struct(f.lit(c).alias(var_name), f.col(c).alias(value_name))
        for c in value_vars
    ))

    # Add to the DataFrame and explode
    _tmp = df.withColumn('_vars_and_vals', f.explode(_vars_and_vals))

    cols = list(id_vars) + [f.col('_vars_and_vals')[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)


def document_total_count(
        df: DataFrame,
        var_name: str) -> DataFrame:
    """Count total documents."""
    out = df.groupBy().count().alias('count')
    out = out.withColumn('datasourceId', f.lit('all'))
    out = out.withColumn('variable', f.lit(var_name))
    out = out.withColumn('field', f.lit(None).cast(t.StringType()))
    return out


def document_count_by(
        df: DataFrame,
        column: str,
        var_name: str) -> DataFrame:
    """Count documents by grouping column."""
    out = df.groupBy(column).count().alias('count')
    out = out.withColumn('variable', f.lit(var_name))
    out = out.withColumn('field', f.lit(None).cast(t.StringType()))
    return out


def not_null_fields_count(df: DataFrame, var_name: str, group_by_datasource: bool) -> DataFrame:
    """Count number of not null values for each field in the dataframe. If `group_by_datasource` is enabled, the
    calculation is performed separately for every datasource in the `datasourceId` column."""
    # Flatten the dataframe schema.
    flat_df = df.select([f.col(c).alias(c) for c in flatten(df.schema)])
    # Get the list of fields to count.
    field_list = flat_df.drop('datasourceId').columns if group_by_datasource else flat_df.columns
    # Count not-null values per field.
    exprs = [
        f.sum(f.when(f.col(field.name).getItem(0).isNotNull(), f.lit(1)).otherwise(f.lit(0))).alias(field.name)
        if isinstance(field.dataType, t.ArrayType)
        else f.sum(f.when(f.col(field.name).isNotNull(), f.lit(1)).otherwise(f.lit(0))).alias(field.name)
        for field in field_list
    ]
    # Group (if necessary) and aggregate.
    df_grouped = df.groupBy(f.col('datasourceId')) if group_by_datasource else df
    df_aggregated = df_grouped.agg(*exprs)
    # Clean column names.
    df_cleaned = df_aggregated.toDF(*(c.replace('.', '_') for c in df_aggregated.columns))
    # Wide to long format.
    melted = melt(df_cleaned, id_vars=['datasourceId'] if group_by_datasource else [], var_name='field',
                  value_vars=field_list, value_name='count')
    melted = melted.withColumn('variable', f.lit(var_name))
    if not group_by_datasource:
        melted = melted.withColumn('datasourceId', f.lit('all'))
    return melted


def evidence_distinct_fields_count(
        df: DataFrame,
        var_name: str) -> DataFrame:
    """Count unique values in variable (e.g. targetId) and datasource."""

    # flatten dataframe schema
    flat_df = df.select([f.col(c).alias(c) for c in flatten(df.schema)])
    # Unique counts per column field
    exprs = [f.countDistinct(f.col(field.name)).alias(field.name)
             for field in list(filter(lambda x: x.name != 'datasourceId',
                                      flat_df.schema))]
    out = df.groupBy(f.col('datasourceId')).agg(*exprs)
    # Clean column names
    out_cleaned = out.toDF(*(c.replace('.', '_') for c in out.columns))
    # Clean  column names
    cols = [c.name for c in filter(lambda x: x.name != 'datasourceId',
                                   out_cleaned.schema.fields)]
    melted = melt(out_cleaned,
                  id_vars=['datasourceId'],
                  var_name='field',
                  value_vars=cols,
                  value_name='count')
    melted = melted.withColumn('variable', f.lit(var_name))
    return melted


def read_file_if_provided(spark, filename):
    if filename:
        return spark.read.json(filename) if 'json' in filename else spark.read.parquet(filename)


def parse_args():
    """Load command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)

    # General required arguments.
    required_arguments = parser.add_argument_group('required arguments')
    required_arguments.add_argument(
        '--run-id', required=True, type=str, help=(
            'Pipeline run identifier to be stored in the runId column. This should be of the format YY.MM.RUN-TYPE, '
            'for example: 20.04.1-pre.'))
    required_arguments.add_argument(
        '--out', required=True, metavar='<path>', type=str, help=(
            'Output filename with the release metrics in the CSV format. For consistency, the basename should match '
            'the run ID provided in --run-id. For example: data/20.04.1-pre.csv.'))

    # General optional arguments.
    parser.add_argument(
        '--log-file', required=False, type=str, help=(
            'Destination of the logs generated by this script.'))

    # The evidence dataset. Can be supplied in different formats depending on whether we are running this before or
    # after the ETL pipeline.
    evidence = parser.add_mutually_exclusive_group()
    evidence.add_argument('--evidence-pre-pipeline', required=False, metavar='<path>', type=str, help=(
        'Directory containing all gzipped JSON files with the submitted evidence strings. Files from the ' 
        'subdirectories are also recursively collected.'))
    evidence.add_argument('--evidence-post-pipeline', required=False, metavar='<path>', type=str, help=(
        'Evidence files from ${ETL_PARQUET_OUTPUT_ROOT}/evidence.'))

    # All of the remaining datasets.
    parser.add_argument(
        '--evidence-failed', required=False, metavar='<path>', type=str, help=(
            'Failed evidence files from ${ETL_PARQUET_OUTPUT_ROOT}/evidenceFailed.'))
    parser.add_argument(
        '--associations-direct', required=False, metavar='<path>', type=str, help=(
            'Direct association files from ${ETL_PARQUET_OUTPUT_ROOT}/associationByOverallDirect.'))
    parser.add_argument(
        '--associations-indirect', required=False, metavar='<path>', type=str, help=(
            'Indirect association files from ${ETL_PARQUET_OUTPUT_ROOT}/associationByOverallIndirect.'))
    parser.add_argument(
        '--diseases', required=False, metavar='<path>', type=str, help=(
            'Disease information from ${ETL_PARQUET_OUTPUT_ROOT}/diseases.'))
    parser.add_argument(
        '--drugs', required=False, metavar='<path>', type=str, help=(
            'ChEMBL dataset directory from ${ETL_INPUT_ROOT}/annotation-files/chembl/chembl_*molecule*.jsonl.'))

    return parser.parse_args()


def get_columns_to_report(dataset_columns):
    return [
        'datasourceId',
        'targetFromSourceId',
        'diseaseFromSourceMappedId' if 'diseaseFromSourceMappedId' in dataset_columns else 'diseaseFromSourceId',
        'drugId',
        'variantId',
        'literature'
    ]


def main(args):
    # Initialise a Spark session.
    spark = SparkSession.builder.getOrCreate()

    # Initialise logging.
    logging_config = {
        'level': logging.INFO,
        'format': '%(name)s - %(levelname)s - %(message)s',
        'datefmt': '%Y-%m-%d %H:%M:%S',
    }
    if args.log_file:
        logging_config['filename'] = args.log_file
    logging.basicConfig(**logging_config)

    # All datasets are optional.
    evidence, evidence_failed, associations_direct, associations_indirect, diseases, drugs = [None] * 6

    # Load data.
    evidence_filename = None
    if args.evidence_pre_pipeline:
        evidence = spark.read.option('recursiveFileLookup', 'true').json(args.evidence_pre_pipeline)
        evidence_filename = args.evidence_pre_pipeline
    elif args.evidence_post_pipeline:
        evidence = read_file_if_provided(spark, args.evidence_post_pipeline)
        evidence_filename = args.evidence_post_pipeline
    evidence_failed = read_file_if_provided(spark, args.evidence_failed)
    associations_direct = read_file_if_provided(spark, args.associations_direct)
    associations_indirect = read_file_if_provided(spark, args.associations_indirect)
    diseases = read_file_if_provided(spark, args.diseases)
    drugs = read_file_if_provided(spark, args.drugs)

    datasets = []

    if evidence:
        logging.info(f'Running metrics from {evidence_filename}.')
        columns_to_report = get_columns_to_report(evidence.columns)

        datasets.extend([
            # Total evidence count.
            document_total_count(evidence, 'evidenceTotalCount'),
            # Evidence count by datasource.
            document_count_by(evidence, 'datasourceId', 'evidenceCountByDatasource'),
            # Number of evidences that have a not null value in the given field.
            not_null_fields_count(evidence, 'evidenceFieldNotNullCountByDatasource', group_by_datasource=True),
            # distinctCount takes some time on all columns: subsetting them.
            evidence_distinct_fields_count(evidence.select(columns_to_report),
                                           'evidenceDistinctFieldsCountByDatasource'),
        ])

    if evidence_failed:
        logging.info(f'Running metrics from {args.evidence_failed}.')
        columns_to_report = get_columns_to_report(evidence_failed.columns)
        datasets.extend([
            # Total invalids.
            document_total_count(evidence_failed,
                                 'evidenceInvalidTotalCount'),
            # Evidence count (duplicates).
            document_total_count(evidence_failed.filter(f.col('markedDuplicate')),
                                 'evidenceDuplicateTotalCount'),
            # Evidence count (targets not resolved).
            document_total_count(evidence_failed.filter(~f.col('resolvedTarget')),
                                 'evidenceUnresolvedTargetTotalCount'),
            # Evidence count (diseases not resolved).
            document_total_count(evidence_failed.filter(~f.col('resolvedDisease')),
                                 'evidenceUnresolvedDiseaseTotalCount'),

            # Evidence count by datasource (invalids).
            document_count_by(evidence_failed,
                              'datasourceId',
                              'evidenceInvalidCountByDatasource'),
            # Evidence count by datasource (duplicates).
            document_count_by(evidence_failed.filter(f.col('markedDuplicate')),
                              'datasourceId',
                              'evidenceDuplicateCountByDatasource'),
            # Evidence count by datasource (targets not resolved).
            document_count_by(evidence_failed.filter(~f.col('resolvedTarget')),
                              'datasourceId',
                              'evidenceUnresolvedTargetCountByDatasource'),
            # Evidence count by datasource (diseases not resolved).
            document_count_by(evidence_failed.filter(~f.col('resolvedDisease')),
                              'datasourceId',
                              'evidenceUnresolvedDiseaseCountByDatasource'),

            # Distinct values in selected fields (invalid evidence).
            evidence_distinct_fields_count(evidence_failed.select(columns_to_report),
                                           'evidenceInvalidDistinctFieldsCountByDatasource'),
            # Evidence count by datasource (duplicates).
            evidence_distinct_fields_count(evidence_failed.filter(f.col('markedDuplicate')).select(columns_to_report),
                                           'evidenceDuplicateDistinctFieldsCountByDatasource'),
            # Evidence count by datasource (targets not resolved).
            evidence_distinct_fields_count(evidence_failed.filter(~f.col('resolvedTarget')).select(columns_to_report),
                                           'evidenceUnresolvedTargetDistinctFieldsCountByDatasource'),
            # Evidence count by datasource (diseases not resolved).
            evidence_distinct_fields_count(evidence_failed.filter(~f.col('resolvedDisease')).select(columns_to_report),
                                           'evidenceUnresolvedDiseaseDistinctFieldsCountByDatasource'),
        ])

    if associations_direct:
        logging.info(f'Running metrics from {args.associations_direct}.')
        datasets.extend([
            # Total association count.
            document_total_count(associations_direct, 'associationsDirectTotalCount'),
            # Associations by datasource.
            document_count_by(
                associations_direct.select(
                    'targetId',
                    'diseaseId',
                    f.explode(f.col('overallDatasourceHarmonicVector.datasourceId')).alias('datasourceId')
                ),
                'datasourceId',
                'associationsDirectByDatasource'
            ),
        ])

    if associations_indirect:
        logging.info(f'Running metrics from {args.associations_indirect}.')
        datasets.extend([
            # Total association count.
            document_total_count(associations_indirect,
                                 'associationsIndirectTotalCount'),
            # Associations by datasource.
            document_count_by(
                associations_indirect.select(
                    'targetId',
                    'diseaseId',
                    f.explode(f.col('overallDatasourceHarmonicVector.datasourceId')).alias('datasourceId')
                ),
                'datasourceId',
                'associationsIndirectByDatasource'),
        ])

    if diseases:
        # TODO: diseases.
        logging.info(f'Running metrics from {args.diseases}.')
        datasets.extend([
            document_total_count(diseases, 'diseasesTotalCount'),
            not_null_fields_count(diseases, 'diseasesNotNullCount', group_by_datasource=False),
        ])

    if drugs:
        # TODO: drugs.
        logging.info(f'Running metrics from {args.drugs}.')
        datasets.extend([
            document_total_count(drugs, 'drugsTotalCount'),
            not_null_fields_count(drugs, 'drugsNotNullCount', group_by_datasource=False),
        ])

    # Write output and clean up.
    metrics = reduce(DataFrame.unionByName, datasets)
    metrics = metrics.withColumn('runId', f.lit(args.run_id))
    metrics.toPandas().to_csv(f'{args.out}', header=True, index=False)
    logging.info(f'{args.out} has been generated. Exiting.')
    spark.stop()


if __name__ == '__main__':
    main(parse_args())
