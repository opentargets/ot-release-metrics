#!/usr/bin/env python3

import argparse
from typing import Iterable
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
import pyspark.sql.functions as f
from pyspark.sql.types import *


# Required to flatten the schema
def flatten(schema, prefix=None):
    """Flatten schema"""
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType
        if isinstance(dtype, StructType):
            fields += flatten(dtype, prefix=name)
        else:
            fields.append(name)
    return fields


def melt(
        df: DataFrame,
        id_vars: Iterable[str], value_vars: Iterable[str],
        var_name: str = "variable", value_name: str = "value") -> DataFrame:
    """Convert :class:`DataFrame` from wide to long format."""

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name))
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
        col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)


def documentTotalCount(
        df: DataFrame,
        var_name: str) -> DataFrame:
    '''Count total documents'''
    out = df.groupBy().count().alias("count")
    out = out.withColumn("datasourceId", lit("all"))
    out = out.withColumn("variable", lit(var_name))
    out = out.withColumn("field", lit(None).cast(StringType()))
    return out


def documentCountBy(
        df: DataFrame,
        column: str,
        var_name: str) -> DataFrame:
    '''Count documents by grouping column'''
    #out = df.withColumn("datasourceId", lit("all"))
    out = df.groupBy(column).count().alias("count")
    out = out.withColumn("variable", lit(var_name))
    out = out.withColumn("field", lit(None).cast(StringType()))
    return out


def evidenceNotNullFieldsCount(
        df: DataFrame,
        var_name: str) -> DataFrame:
    '''Counts number of evidences with not null values in variable.'''
    # flatten dataframe schema
    flatDf = df.select([col(c).alias(c) for c in flatten(df.schema)])

    # counting not-null evidence per field
    exprs = [sum(when(col(f.name).getItem(0).isNotNull(), lit(1))
                 .otherwise(lit(0))).alias(f.name)
             if isinstance(f.dataType, ArrayType)
             else
             sum(when(col(f.name).isNotNull(), lit(1))
                 .otherwise(lit(0))).alias(f.name)
             for f in list(filter(lambda x: x.name != "datasourceId",
                                  flatDf.schema))]
    out = df.groupBy(col("datasourceId")).agg(*exprs)
    # Clean column names
    out_cleaned = out.toDF(*(c.replace('.', '_') for c in out.columns))
    # wide to long format
    cols = out_cleaned.drop("datasourceId").columns

    melted = melt(out_cleaned,
                  id_vars=["datasourceId"],
                  var_name="field",
                  value_vars=cols,
                  value_name="count")
    melted = melted.withColumn("variable", lit(var_name))
    return melted


def evidenceDistinctFieldsCount(
        df: DataFrame,
        var_name: str) -> DataFrame:
    '''Counts unique values in variable (e.g. targetId) and datasource.'''

    # flatten dataframe schema
    flatDf = df.select([col(c).alias(c) for c in flatten(df.schema)])
    # Unique counts per column field
    exprs = [countDistinct(col(f.name)).alias(f.name)
             for f in list(filter(lambda x: x.name != "datasourceId",
                                  flatDf.schema))]
    out = df.groupBy(col("datasourceId")).agg(*exprs)
    # Clean column names
    out_cleaned = out.toDF(*(c.replace('.', '_') for c in out.columns))
    # Clean  column names
    cols = [c.name for c in filter(lambda x: x.name != "datasourceId",
                                   out_cleaned.schema.fields)]
    melted = melt(out_cleaned,
                  id_vars=["datasourceId"],
                  var_name="field",
                  value_vars=cols,
                  value_name="count")
    melted = melted.withColumn("variable", lit(var_name))
    return melted


def parse_args():
    """Load command line arguments."""
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='run_type')

    pre_pipeline_parser = subparsers.add_parser('pre-pipeline', help=(
        'Calculate the metrics for the submitted files (gzipped JSON), before the ETL pipeline is run.'))
    pre_pipeline_parser.add_argument(
        '--evidence-json-dir', required=True, metavar='<path>', type=str, help=(
            'Directory containing all gzipped JSON files with the submitted evidence strings. Files from the '
            'subdirectories are also recursively collected.'))

    post_pipeline_parser = subparsers.add_parser('post-pipeline', help=(
        'Calculate the complete set of metrics after running the ETL pipeline. All parameters are expected to point to '
        'a directory with Parquet files generated by the pipeline for the appropriate dataset. The base Google Storage '
        'path for the pipeline results is gs://open-targets-data-releases/{RELEASE}/output/etl-parquet/.'))
    post_pipeline_parser.add_argument(
        '--evidence', required=False, metavar='<path>', type=str, help=(
            'All evidence files, found in /evidence.'))
    post_pipeline_parser.add_argument(
        '--evidence-failed', required=False, metavar='<path>', type=str, help=(
            'Failed evidence files, found in /evidenceFailed.'))
    post_pipeline_parser.add_argument(
        '--associations-direct', required=False, metavar='<path>', type=str, help=(
            'Direct associations, found in /.'))  # FIXME: TBC.
    post_pipeline_parser.add_argument(
        '--associations-indirect', required=False, metavar='<path>', type=str, help=(
            'Indirect associations, found in /.'))  # FIXME: TBC.
    post_pipeline_parser.add_argument(
        '--diseases', required=False, metavar='<path>', type=str, help=(
            'Disease information, found in /diseases.'))

    required_arguments = parser.add_argument_group('required arguments')
    required_arguments.add_argument(
        '--run-id', required=True, type=str, help=(
            'Pipeline run identifier to be stored in the runId column, for example: 20.04.1.'))
    required_arguments.add_argument(
        '--out', required=True, metavar='<path>', type=str, help=(
            'Output filename with the release metrics in the CSV format.'))

    return parser.parse_args()


def read_parquet_if_provided(spark, parquet_path):
    if parquet_path:
        return spark.read.parquet(parquet_path)
    else:
        return None


def main(args):
    spark = SparkSession.builder.getOrCreate()

    # All datasets are optional.
    evidence, evidence_failed, associations_direct, associations_indirect, diseases = [None] * 5

    # Load data.
    if args.run_type == 'pre-pipeline':
        evidence = spark.read.option('recursiveFileLookup', 'true').json(args.evidence_json_dir)
    elif args.run_type == 'post-pipeline':
        evidence = read_parquet_if_provided(spark, args.evidence)
        evidence_failed = read_parquet_if_provided(spark, args.evidence_failed)
        associations_direct = read_parquet_if_provided(spark, args.associations_direct)
        associations_indirect = read_parquet_if_provided(spark, args.associations_indirect)
        diseases = read_parquet_if_provided(spark, args.diseases)
    else:
        raise AssertionError(f'Incorrect pipeline run type: {args.run_type}.')

    columns_to_report = [
        'datasourceId',
        'targetFromSourceId',
        # TODO: Remove this from 21.04 on.
        'diseaseFromSourceMappedId' if 'diseaseFromSourceMappedId' in evidence.columns else 'diseaseFromSourceId',
        'drugId',
        'variantId',
        'literature'
    ]

    datasets = []

    if evidence:
        datasets.extend([
            # Total evidence count.
            documentTotalCount(evidence, 'evidenceTotalCount'),
            # Evidence count by datasource.
            documentCountBy(evidence, 'datasourceId', 'evidenceCountByDatasource'),
            # Number of evidences that have a not null value in the given field.
            evidenceNotNullFieldsCount(evidence, 'evidenceFieldNotNullCountByDatasource'),
            # distinctCount takes some time on all columns: subsetting them.
            evidenceDistinctFieldsCount(evidence.select(columns_to_report), 'evidenceDistinctFieldsCountByDatasource'),
        ])

    if evidence_failed:
        datasets.extend([
            # Total invalids.
            documentTotalCount(evidence_failed,
                               'evidenceInvalidTotalCount'),
            # Evidence count (duplicates).
            documentTotalCount(evidence_failed.filter(f.col('markedDuplicate')),
                               'evidenceDuplicateTotalCount'),
            # Evidence count (targets not resolved).
            documentTotalCount(evidence_failed.filter(~f.col('resolvedTarget')),
                               'evidenceUnresolvedTargetTotalCount'),
            # Evidence count (diseases not resolved).
            documentTotalCount(evidence_failed.filter(~f.col('resolvedDisease')),
                               'evidenceUnresolvedDiseaseTotalCount'),

            # Evidence count by datasource (invalids).
            documentCountBy(evidence_failed,
                            'datasourceId',
                            'evidenceInvalidCountByDatasource'),
            # Evidence count by datasource (duplicates).
            documentCountBy(evidence_failed.filter(f.col('markedDuplicate')),
                            'datasourceId',
                            'evidenceDuplicateCountByDatasource'),
            # Evidence count by datasource (targets not resolved).
            documentCountBy(evidence_failed.filter(~f.col('resolvedTarget')),
                            'datasourceId',
                            'evidenceUnresolvedTargetCountByDatasource'),
            # Evidence count by datasource (diseases not resolved).
            documentCountBy(evidence_failed.filter(~f.col('resolvedDisease')),
                            'datasourceId',
                            'evidenceUnresolvedDiseaseCountByDatasource'),

            # Distinct values in selected fields (invalid evidence).
            evidenceDistinctFieldsCount(evidence_failed.select(columns_to_report),
                                        'evidenceInvalidDistinctFieldsCountByDatasource'),
            # Evidence count by datasource (duplicates).
            evidenceDistinctFieldsCount(evidence_failed.filter(f.col('markedDuplicate')).select(columns_to_report),
                                        'evidenceDuplicateDistinctFieldsCountByDatasource'),
            # Evidence count by datasource (targets not resolved).
            evidenceDistinctFieldsCount(evidence_failed.filter(~f.col('resolvedTarget')).select(columns_to_report),
                                        'evidenceUnresolvedTargetDistinctFieldsCountByDatasource'),
            # Evidence count by datasource (diseases not resolved).
            evidenceDistinctFieldsCount(evidence_failed.filter(~f.col('resolvedDisease')).select(columns_to_report),
                                        'evidenceUnresolvedDiseaseDistinctFieldsCountByDatasource'),
        ])

    if associations_direct:
        datasets.extend([
            # Total association count.
            documentTotalCount(associations_direct, 'associationsDirectTotalCount'),
            # Associations by datasource.
            documentCountBy(
                associations_direct.select(
                    'targetId',
                    'diseaseId',
                    f.explode(f.col('overallDatasourceHarmonicScoreDSs.datasourceId')).alias('datasourceId')
                ),
                'datasourceId',
                'associationsDirectByDatasource'
            ),
        ])

    if associations_indirect:
        datasets.extend([
            # Total association count.
            documentTotalCount(associations_indirect,
                               'associationsIndirectTotalCount'),
            # Associations by datasource.
            documentCountBy(
                associations_indirect.select(
                    'targetId',
                    'diseaseId',
                    f.explode(f.col('overallDatasourceHarmonicScoreDSs.datasourceId')).alias('datasourceId')
                ),
                'datasourceId',
                'associationsIndirectByDatasource'),
        ])

    if diseases:
        # TODO: diseases.
        datasets.extend([
            documentTotalCount(diseases, 'diseaseTotalCount')
        ])

    # TODO: drugs.

    # Write output and clean up.
    metrics = reduce(DataFrame.unionByName, datasets)
    metrics = metrics.withColumn('runId', f.lit(args.run_id))
    metrics.toPandas().to_csv(f'{args.out}', header=True, index=False)
    spark.stop()


if __name__ == '__main__':
    main(parse_args())
