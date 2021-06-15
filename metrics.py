#!/usr/bin/env python3
"""Calculates the various quality metrics for evidence and the associated datasets.

For the post-pipeline run, the ${ETL_INPUT_ROOT} is gs://open-targets-data-releases/${RELEASE}/input/.
The ${ETL_PARQUET_OUTPUT_ROOT} is gs://ot-snapshots/etl/outputs/${RELEASE}/parquet/ for the snapshots (releases in
progress), and gs://open-targets-data-releases/${RELEASE}/output/etl-parquet/ for the completed releases."""

import argparse
from functools import reduce
import logging
import logging.config
import os.path
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
        id_vars: Iterable[str],
        value_vars: Iterable[str],
        var_name: str = 'variable',
        value_name: str = 'value'
) -> DataFrame:
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
        var_name: str
) -> DataFrame:
    """Count total documents."""
    out = df.groupBy().count().alias('count')
    out = out.withColumn('datasourceId', f.lit('all'))
    out = out.withColumn('variable', f.lit(var_name))
    out = out.withColumn('field', f.lit(None).cast(t.StringType()))
    return out


def document_count_by(
        df: DataFrame,
        column: str,
        var_name: str
) -> DataFrame:
    """Count documents by grouping column."""
    out = df.groupBy(column).count().alias('count')
    out = out.withColumn('variable', f.lit(var_name))
    out = out.withColumn('field', f.lit(None).cast(t.StringType()))
    return out


def not_null_fields_count(
        df: DataFrame,
        var_name: str,
        group_by_datasource: bool
) -> DataFrame:
    """Count number of not null values for each field in the dataframe. If `group_by_datasource` is enabled, the
    calculation is performed separately for every datasource in the `datasourceId` column."""
    # Flatten the dataframe schema.
    flat_df = df.select([f.col(c).alias(c) for c in flatten(df.schema)])
    # Get the list of fields to count.
    field_list = flat_df.schema
    if group_by_datasource:
        field_list = [field for field in field_list if field.name != 'datasourceId']
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
    melted = (
        melt(
            df=df_cleaned,
            id_vars=['datasourceId'] if group_by_datasource else [],
            var_name='field',
            value_vars=df_cleaned.drop('datasourceId').columns if group_by_datasource else df_cleaned.columns,
            value_name='count'
        )
        .withColumn('variable', f.lit(var_name))
    )
    if not group_by_datasource:
        melted = melted.withColumn('datasourceId', f.lit('all'))
    return melted


def evidence_distinct_fields_count(
        df: DataFrame,
        var_name: str
) -> DataFrame:
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


def read_path_if_provided(spark, path):
    """Automatically detect the format of the input data and read it into the Spark dataframe. The supported formats
    are: a single JSON file; a directory with JSON files; a directory with Parquet files."""
    # All datasets are optional.
    if path is None:
        return None

    # The provided path must exist and must be either a file or a directory.
    assert os.path.exists(path), f'The provided path {path} does not exist.'
    assert os.path.isdir(path) or os.path.isfile(path), \
        f'The provided path {path} is neither a file or a directory.'

    # Case 1: We are provided with a single file.
    if os.path.isfile(path):
        # In this case, this must be a (possibly compressed) JSON.
        if path.endswith(('.json', '.json.gz', '.jsonl', '.jsonl.gz')):
            return spark.read.json(path)
        else:
            raise AssertionError(f'The format of the provided file {path} is not supported.')

    # Case 2: We are provided with a directory. Let's peek inside to see what it contains.
    all_files = [
        os.path.join(dp, filename)
        for dp, dn, filenames in os.walk(path)
        for filename in filenames
    ]

    # It must be either exclusively JSON, or exclusively Parquet.
    json_files = [fn for fn in all_files if fn.endswith(('.json', '.json.gz', '.jsonl', '.jsonl.gz'))]
    parquet_files = [fn for fn in all_files if fn.endswith('.parquet')]
    assert not(json_files and parquet_files), f'The provided directory {path} contains a mix of JSON and Parquet.'
    assert json_files or parquet_files, f'The provided directory {path} contains neither JSON nor Parquet.'

    # A directory with JSON files.
    if json_files:
        return spark.read.option('recursiveFileLookup', 'true').json(path)

    # A directory with Parquet files.
    if parquet_files:
        return spark.read.parquet(path)


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

    # Dataset specification arguments.
    dataset_arguments = parser.add_argument_group('dataset specification arguments (all optional)')
    dataset_arguments.add_argument('--evidence', required=False, metavar='<path>', type=str, help=(
        'The directory containing the evidence files. Depending on when this script is being run, this can be either '
        'of the two possible formats.\n\nWhen running before the ETL pipeline, this directory is expected to contain '
        'all gzipped JSON files with the original submitted evidence strings. Files from the subdirectories will also '
        'be recursively collected.\n\nWhen running after the ETL pipeline, this directory is expected to contain the '
        'processed evidence in Parquet format from ${ETL_PARQUET_OUTPUT_ROOT}/evidence.'))
    dataset_arguments.add_argument(
        '--evidence-failed', required=False, metavar='<path>', type=str, help=(
            'Failed evidence files from ${ETL_PARQUET_OUTPUT_ROOT}/evidenceFailed.'))
    dataset_arguments.add_argument(
        '--associations-direct', required=False, metavar='<path>', type=str, help=(
            'Direct association files from ${ETL_PARQUET_OUTPUT_ROOT}/associationByDatasourceDirect.'))
    dataset_arguments.add_argument(
        '--associations-indirect', required=False, metavar='<path>', type=str, help=(
            'Indirect association files from ${ETL_PARQUET_OUTPUT_ROOT}/associationByDatasourceIndirect.'))
    dataset_arguments.add_argument(
        '--diseases', required=False, metavar='<path>', type=str, help=(
            'Disease information from ${ETL_PARQUET_OUTPUT_ROOT}/diseases.'))
    dataset_arguments.add_argument(
        '--targets', required=False, metavar='<path>', type=str, help=(
            'Targets dataset from ${ETL_PARQUET_OUTPUT_ROOT}/targets.'))
    dataset_arguments.add_argument(
        '--drugs', required=False, metavar='<path>', type=str, help=(
            'ChEMBL dataset from ${ETL_INPUT_ROOT}/annotation-files/chembl/chembl_*molecule*.jsonl.'))

    # General optional arguments.
    parser.add_argument(
        '--log-file', required=False, type=str, help=(
            'Destination of the logs generated by this script.'))

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

    # Load data. All datasets are optional.
    evidence = read_path_if_provided(spark, args.evidence)
    evidence_failed = read_path_if_provided(spark, args.evidence_failed)
    associations_direct = read_path_if_provided(spark, args.associations_direct)
    associations_indirect = read_path_if_provided(spark, args.associations_indirect)
    diseases = read_path_if_provided(spark, args.diseases)
    targets = read_path_if_provided(spark, args.targets)
    drugs = read_path_if_provided(spark, args.drugs)

    datasets = []

    if evidence:
        logging.info(f'Running metrics from {args.evidence}.')
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
            document_total_count(associations_direct.select("diseaseId", "targetId").distinct(), 'associationsDirectTotalCount'),
            # Associations by datasource.
            document_count_by(
                associations_direct,
                'datasourceId',
                'associationsDirectByDatasource'
            ),
        ])

    if associations_indirect:
        logging.info(f'Running metrics from {args.associations_indirect}.')
        datasets.extend([
            # Total association count.
            document_total_count(associations_indirect.select("diseaseId", "targetId").distinct(),
                                 'associationsIndirectTotalCount'),
            # Associations by datasource.
            document_count_by(
                associations_indirect,
                'datasourceId',
                'associationsIndirectByDatasource'),
        ])

    if diseases:
        logging.info(f'Running metrics from {args.diseases}.')
        datasets.extend([
            document_total_count(diseases, 'diseasesTotalCount'),
            not_null_fields_count(diseases, 'diseasesNotNullCount', group_by_datasource=False),
        ])

    if targets:
        logging.info(f'Running metrics from {args.targets}.')
        datasets.extend([
            document_total_count(targets, 'targetsTotalCount'),
        ])

    if drugs:
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
