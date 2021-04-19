#!/usr/bin/env python3

import argparse
from typing import Iterable
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.appName('spark').getOrCreate()

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
    """ Load command line args """
    parser = argparse.ArgumentParser()
    parser.add_argument('--runId',
                        help=('Pipeline run identifier'),
                        type=str,
                        required=True)
    parser.add_argument('--disease',
                        metavar="<path>",
                        help=('Disease path'),
                        type=str,
                        required=False)
    parser.add_argument('--evidence',
                        metavar="<path>",
                        help=('Evidence path'),
                        type=str,
                        required=False)
    parser.add_argument('--failedEvidence',
                        metavar="<path>",
                        help=('Evidence failing path'),
                        type=str,
                        required=False)
    parser.add_argument('--directAssociations',
                        metavar="<path>",
                        help=('Direct associations'),
                        type=str,
                        required=False)
    parser.add_argument('--indirectAssociations',
                        metavar="<path>",
                        help=('Indirect associations'),
                        type=str,
                        required=False)
    parser.add_argument('--out',
                        metavar="<path>",
                        help=("Output path"),
                        type=str,
                        required=True)
    parser.add_argument('--local',
                        help="run local[*]",
                        action='store_true',
                        required=False,
                        default=True)
    parser.add_argument('--prePipeline',
                        help="State whether the data is prior to the pipeline run",
                        action='store_true',
                        required=False)
    args = parser.parse_args()
    return args

def main(args):
    sparkConf = SparkConf()

    if args.local:
        spark = (
            SparkSession.builder
            .config(conf=sparkConf)
            .master('local[*]')
            .getOrCreate()
        )
    else:
        spark = (
            SparkSession.builder
            .config(conf=sparkConf)
            .getOrCreate()
        )

    # Load data
    if args.prePipeline:
        evd = spark.read.option("recursiveFileLookup","true").json(args.evidence)
    elif args.disease:
        dis = spark.read.json(args.disease)
    else:
        evd = spark.read.parquet(args.evidence)
        evdBad = spark.read.parquet(args.failedEvidence)
        assDirect = spark.read.parquet(args.directAssociations)
        assIndirect = spark.read.parquet(args.indirectAssociations)

    if "diseaseFromSourceMappedId" in evd.columns: # TODO: Remove this from 21.04 on
        columnsToReport = ["datasourceId", "targetFromSourceId", "diseaseFromSourceMappedId", "drugId", "variantId", "literature"]
    else:
        columnsToReport = ["datasourceId", "targetFromSourceId", "diseaseFromSourceId", "drugId", "variantId", "literature"]

    datasets = [
        # VALID EVIDENCE
        # Total evidence count
        documentTotalCount(evd, "evidenceTotalCount"),
        # Evidence count by datasource
        documentCountBy(evd, "datasourceId", "evidenceCountByDatasource"),
        # Number of evidences that have a not null value in the given field
        evidenceNotNullFieldsCount(evd,
                            "evidenceFieldNotNullCountByDatasource"),
        # distinctCount takes some time on all columns: subsetting them
        evidenceDistinctFieldsCount(evd.select(columnsToReport),
                                    "evidenceDistinctFieldsCountByDatasource")

        # INVALID EVIDENCE
        # Total invalids
        documentTotalCount(evdBad, "evidenceInvalidTotalCount"),
        # Evidence count (duplicates)
        documentTotalCount(evdBad.filter(F.col("markedDuplicate")),
                           "evidenceDuplicateTotalCount"),
        # Evidence count (targets not resolved)
        documentTotalCount(evdBad.filter(~F.col("resolvedTarget")),
                           "evidenceUnresolvedTargetTotalCount"),
        # Evidence count (diseases not resolved)
        documentTotalCount(evdBad.filter(~F.col("resolvedDisease")),
                           "evidenceUnresolvedDiseaseTotalCount"),

        # Evidence count by datasource (invalids)
        documentCountBy(evdBad, "datasourceId",
                        "evidenceInvalidCountByDatasource"),
        # Evidence count by datasource (duplicates)
        documentCountBy(evdBad.filter(F.col("markedDuplicate")), "datasourceId",
                        "evidenceDuplicateCountByDatasource"),
        # Evidence count by datasource (targets not resolved)
        documentCountBy(evdBad.filter(~F.col("resolvedTarget")),
                        "datasourceId",
                        "evidenceUnresolvedTargetCountByDatasource"),
        # Evidence count by datasource (diseases not resolved)
        documentCountBy(evdBad.filter(~F.col("resolvedDisease")),
                        "datasourceId",
                        "evidenceUnresolvedDiseaseCountByDatasource"),
        # Distinct values in selected fields (invalid evidence)
        evidenceDistinctFieldsCount(
            evdBad
            .select(columnsToReport),
            "evidenceInvalidDistinctFieldsCountByDatasource"),
        # Evidence count by datasource (duplicates)
        evidenceDistinctFieldsCount(
            evdBad
            .filter(F.col("markedDuplicate"))
            .select(columnsToReport),
            "evidenceDuplicateDistinctFieldsCountByDatasource"),
        # Evidence count by datasource (targets not resolved)
        evidenceDistinctFieldsCount(
            evdBad
            .filter(~F.col("resolvedTarget"))
            .select(columnsToReport),
            "evidenceUnresolvedTargetDistinctFieldsCountByDatasource"),
        # Evidence count by datasource (diseases not resolved)
        evidenceDistinctFieldsCount(
            evdBad
            .filter(~F.col("resolvedDisease"))
            .select(columnsToReport),
            "evidenceUnresolvedDiseaseDistinctFieldsCountByDatasource"),

        # DIRECT ASSOCIATIONS
        # Total association count
        documentTotalCount(assDirect, "associationsDirectTotalCount"),
        # Associations by datasource
        documentCountBy(
            assDirect
            .select(
                "targetId",
                "diseaseId",
                F.explode(
                    F.col("overallDatasourceHarmonicScoreDSs.datasourceId"))
                .alias("datasourceId")),
            "datasourceId",
            "associationsDirectByDatasource"),
        # INDIRECT ASSOCIATIONS
        # Total association count
        documentTotalCount(assIndirect,
                           "associationsIndirectTotalCount"),
        # Associations by datasource
        documentCountBy(
            assIndirect
            .select("targetId",
                    "diseaseId",
                    F.explode(
                        F.col(
                            "overallDatasourceHarmonicScoreDSs.datasourceId"))
                    .alias("datasourceId")),
            "datasourceId",
            "associationsIndirectByDatasource"),

        # TODO: DISEASE
        documentTotalCount(disease, "diseaseTotalCount")
        # TODO: DRUG
    ]

    metrics = reduce(DataFrame.unionByName, datasets)
    metrics = metrics.withColumn("runId", lit(args.runId))

    # Write output
    metrics.toPandas().to_csv(f"{args.out}", header=True, index=False)

    # clean up
    spark.stop()
    return 0


if __name__ == '__main__':
    args = parse_args()
    exit(main(args))
