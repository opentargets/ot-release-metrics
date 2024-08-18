#!/usr/bin/env python3
"""Calculates the various quality metrics for evidence and the associated datasets."""

from __future__ import annotations

from collections import namedtuple
from functools import reduce
import logging
import logging.config
import os
from typing import Iterable, TYPE_CHECKING

import hydra
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
import pyspark.sql.types as t

from src.metric_calculation.utils import (
    initialize_spark_session,
    read_path_if_provided,
    fetch_pre_etl_evidence,
    detect_release_timestamp,
    write_metrics_to_csv,
    write_metrics_to_hf_hub
)

if TYPE_CHECKING:
    from omegaconf import DictConfig


def flatten(schema, prefix=None):
    """
    It takes a Spark schema and returns a list of all fields in the schema once flattened.

    Args:
      schema: The schema of the dataframe
      prefix: The prefix to prepend to the field names.

    Returns:
      A list of all the columns in the dataframe.
    """
    fields = []
    for field in schema.fields:
        name = f"{prefix}.{field.name}" if prefix else field.name
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
    var_name: str = "variable",
    value_name: str = "value",
) -> DataFrame:
    """
    It takes a DataFrame, and a list of columns to melt, and returns a DataFrame with the columns melted
    into rows

    Args:
      df (DataFrame): DataFrame
      id_vars (Iterable[str]): The columns that will be kept as-is.
      value_vars (Iterable[str]): The columns that you want to melt.
      var_name (str): The name of the column that will contain the variable names. Defaults to variable
      value_name (str): The name of the column that will contain the values. Defaults to value

    Returns:
      A dataframe with the columns id_vars, var_name, and value_name.
    """
    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = f.array(
        *(
            f.struct(f.lit(c).alias(var_name), f.col(c).alias(value_name))
            for c in value_vars
        )
    )

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", f.explode(_vars_and_vals))

    cols = list(id_vars) + [
        f.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]
    ]
    return _tmp.select(*cols)


def document_total_count(df: DataFrame, var_name: str) -> DataFrame:
    """Count total documents."""
    out = df.groupBy().count().withColumnRenamed("count", "value")
    out = out.withColumn("datasourceId", f.lit("all"))
    out = out.withColumn("variable", f.lit(var_name))
    out = out.withColumn("field", f.lit(None).cast(t.StringType()))
    return out


def document_count_by(df: DataFrame, column: str, var_name: str) -> DataFrame:
    """Count documents by grouping column."""
    out = df.groupBy(column).count().withColumnRenamed("count", "value")
    out = out.withColumn("variable", f.lit(var_name))
    out = out.withColumn("field", f.lit(None).cast(t.StringType()))
    return out


def not_null_fields_count(
    df: DataFrame, var_name: str, group_by_datasource: bool
) -> DataFrame:
    """Count number of not null values for each field in the dataframe. If `group_by_datasource` is enabled, the
    calculation is performed separately for every datasource in the `datasourceId` column.
    """
    # Flatten the dataframe schema.
    flat_df = df.select([f.col(c).alias(c) for c in flatten(df.schema)])
    # Get the list of fields to count.
    field_list = flat_df.schema
    if group_by_datasource:
        field_list = [field for field in field_list if field.name != "datasourceId"]
    # Count not-null values per field.
    exprs = [
        f.sum(
            f.when(f.col(field.name).getItem(0).isNotNull(), f.lit(1)).otherwise(
                f.lit(0)
            )
        ).alias(field.name)
        if isinstance(field.dataType, t.ArrayType)
        else f.sum(
            f.when(f.col(field.name).isNotNull(), f.lit(1)).otherwise(f.lit(0))
        ).alias(field.name)
        for field in field_list
    ]
    # Group (if necessary) and aggregate.
    df_grouped = df.groupBy(f.col("datasourceId")) if group_by_datasource else df
    df_aggregated = df_grouped.agg(*exprs)
    # Clean column names.
    df_cleaned = df_aggregated.toDF(
        *(c.replace(".", "_") for c in df_aggregated.columns)
    )
    # Wide to long format.
    melted = melt(
        df=df_cleaned,
        id_vars=["datasourceId"] if group_by_datasource else [],
        var_name="field",
        value_vars=df_cleaned.drop("datasourceId").columns
        if group_by_datasource
        else df_cleaned.columns,
        value_name="value",
    ).withColumn("variable", f.lit(var_name))
    if not group_by_datasource:
        melted = melted.withColumn("datasourceId", f.lit("all"))
    return melted


def evidence_distinct_fields_count(df: DataFrame, var_name: str) -> DataFrame:
    """Count unique values in variable (e.g. targetId) and datasource."""

    # flatten dataframe schema
    flat_df = df.select([f.col(c).alias(c) for c in flatten(df.schema)])
    # Unique counts per column field
    exprs = [
        f.countDistinct(f.col(field.name)).alias(field.name)
        for field in list(filter(lambda x: x.name != "datasourceId", flat_df.schema))
    ]
    out = df.groupBy(f.col("datasourceId")).agg(*exprs)
    # Clean column names
    out_cleaned = out.toDF(*(c.replace(".", "_") for c in out.columns))
    # Clean  column names
    cols = [
        c.name
        for c in filter(lambda x: x.name != "datasourceId", out_cleaned.schema.fields)
    ]
    melted = melt(
        out_cleaned,
        id_vars=["datasourceId"],
        var_name="field",
        value_vars=cols,
        value_name="value",
    )
    melted = melted.withColumn("variable", f.lit(var_name))
    return melted


def auc(associations, score_column_name):
    """
    It calculates the area under the curve for a given set of associations and a score column.

    Args:
      associations: the DataFrame of associations
      score_column_name: The name of the column in the associations DataFrame that contains the score.

    Returns:
      The area under the ROC curve.
    """
    return BinaryClassificationMetrics(
        associations.select(score_column_name, "gold_standard").rdd.map(list)
    ).areaUnderROC


def odds_ratio(associations, datasource):
    """
    It calculates the odds ratio of the associations in the given datasource

    Args:
      associations: the dataframe of associations
      datasource: the datasource we're interested in

    Returns:
      The odds ratio of the gold standard associations to the non-gold standard associations.
    """
    a = associations.filter(
        (f.col("gold_standard") == 1.0) & (f.col("datasourceId") == datasource)
    ).count()
    b = associations.filter(
        (f.col("gold_standard") == 0.0) & (f.col("datasourceId") == datasource)
    ).count()
    c = associations.filter(
        (f.col("gold_standard") == 1.0) & (f.col("datasourceId") != datasource)
    ).count()
    d = associations.filter(
        (f.col("gold_standard") == 0.0) & (f.col("datasourceId") != datasource)
    ).count()
    return a * d / (b * c)


def gold_standard_benchmark(associations: DataFrame, associations_type: str) -> list:
    """Run a benchmark of associations against a known gold standard.

    Args:
        associations: A Spark dataframe with associations for all datasets.
        associations_type: Can be either "Direct" or "Indirect", used to compute the names of metrics.

    Returns:
        A list containing two sets of metrics, AUC and OR, both for each dataframe and overall.
    """

    if "Overall" in associations_type:
        auc_metrics = [
            {
                "value": auc(associations, "score"),
                "datasourceId": "all",
                "variable": f"associations{associations_type}AUC",
                "field": "",
            }
        ]
        or_metrics = [
            {
                "value": 1.0,
                "datasourceId": "all",
                "variable": f"associations{associations_type}OR",
                "field": "",
            }
        ]
    else:
        datasource_names = (
            associations.select("datasourceId")
            .distinct()
            .toPandas()["datasourceId"]
            .unique()
        )
        auc_metrics = [
            {
                "value": auc(
                    associations.filter(f.col("datasourceId") == datasource), "score"
                ),
                "datasourceId": datasource,
                "variable": f"associations{associations_type}AUC",
                "field": "",
            }
            for datasource in datasource_names
        ]
        or_metrics = [
            {
                "value": odds_ratio(associations, datasource),
                "datasourceId": datasource,
                "variable": f"associations{associations_type}OR",
                "field": "",
            }
            for datasource in datasource_names
        ]

    return spark.createDataFrame(auc_metrics + or_metrics)


def get_columns_to_report(dataset_columns):
    """
    It returns a list of columns that we want to report in the final output

    Args:
      dataset_columns: the columns in the dataset

    Returns:
      A list of columns to report.
    """
    return [
        "datasourceId",
        "targetFromSourceId",
        "diseaseFromSourceMappedId"
        if "diseaseFromSourceMappedId" in dataset_columns
        else "diseaseFromSourceId",
        "drugId",
        "variantId",
        "literature",
    ]


def calculate_additional_post_etl_metrics(metrics_cfg):
    evidence_failed = read_path_if_provided(metrics_cfg.datasets.evidence_failed)
    associations_direct = read_path_if_provided(
        metrics_cfg.datasets.associations_source_direct
    )
    associations_indirect = read_path_if_provided(
        metrics_cfg.datasets.associations_source_indirect
    )
    associations_overall_direct = read_path_if_provided(
        metrics_cfg.datasets.associations_overall_direct
    )
    associations_overall_indirect = read_path_if_provided(
        metrics_cfg.datasets.associations_overall_indirect
    )
    diseases = read_path_if_provided(metrics_cfg.datasets.diseases)
    targets = read_path_if_provided(metrics_cfg.datasets.targets)
    drugs = read_path_if_provided(metrics_cfg.datasets.drugs)
    gold_standard_associations = read_path_if_provided(
        metrics_cfg.gold_standard.associations
    )
    gold_standard_mappings = read_path_if_provided(
        metrics_cfg.gold_standard.efo_mappings
    )

    gold_standard = None
    if gold_standard_associations and gold_standard_mappings:
        gold_standard = (
            gold_standard_associations.join(
                gold_standard_mappings,
                on=gold_standard_associations.MSH == gold_standard_mappings.mesh_label,
            )
            .filter(f.col("`Phase.Latest`") == "Approved")
            .select(
                f.col("ensembl_id").alias("targetId"),
                f.col("efo_id").alias("diseaseId"),
            )
            .withColumn("diseaseId", f.regexp_replace("diseaseId", ":", "_"))
            .withColumn("gold_standard", f.lit(1.0))
        )

    datasets = []

    if evidence_failed:
        logging.info(f"Running metrics from {metrics_cfg.datasets.evidence_failed}.")
        columns_to_report = get_columns_to_report(evidence_failed.columns)
        datasets.extend(
            [
                # Total invalids.
                document_total_count(evidence_failed, "evidenceInvalidTotalCount"),
                # Evidence count (duplicates).
                document_total_count(
                    evidence_failed.filter(f.col("markedDuplicate")),
                    "evidenceDuplicateTotalCount",
                ),
                # Evidence count (nullified score).
                document_total_count(
                    evidence_failed.filter(f.col("nullifiedScore")),
                    "evidenceNullifiedScoreTotalCount",
                ),
                # Evidence count (targets not resolved).
                document_total_count(
                    evidence_failed.filter(~f.col("resolvedTarget")),
                    "evidenceUnresolvedTargetTotalCount",
                ),
                # Evidence count (diseases not resolved).
                document_total_count(
                    evidence_failed.filter(~f.col("resolvedDisease")),
                    "evidenceUnresolvedDiseaseTotalCount",
                ),
                # Total invalids by datasource.
                document_count_by(
                    evidence_failed, "datasourceId", "evidenceInvalidCountByDatasource"
                ),
                # Evidence count by datasource (duplicates).
                document_count_by(
                    evidence_failed.filter(f.col("markedDuplicate")),
                    "datasourceId",
                    "evidenceDuplicateCountByDatasource",
                ),
                # Evidence count by datasource (nullified score).
                document_count_by(
                    evidence_failed.filter(f.col("nullifiedScore")),
                    "datasourceId",
                    "evidenceNullifiedScoreCountByDatasource",
                ),
                # Evidence count by datasource (targets not resolved).
                document_count_by(
                    evidence_failed.filter(~f.col("resolvedTarget")),
                    "datasourceId",
                    "evidenceUnresolvedTargetCountByDatasource",
                ),
                # Evidence count by datasource (diseases not resolved).
                document_count_by(
                    evidence_failed.filter(~f.col("resolvedDisease")),
                    "datasourceId",
                    "evidenceUnresolvedDiseaseCountByDatasource",
                ),
                # Distinct values in selected fields (total invalid evidence).
                evidence_distinct_fields_count(
                    evidence_failed.select(columns_to_report),
                    "evidenceInvalidDistinctFieldsCountByDatasource",
                ),
                # Evidence count by datasource (duplicates).
                evidence_distinct_fields_count(
                    evidence_failed.filter(f.col("markedDuplicate")).select(
                        columns_to_report
                    ),
                    "evidenceDuplicateDistinctFieldsCountByDatasource",
                ),
                # Evidence count by datasource (nullified score).
                evidence_distinct_fields_count(
                    evidence_failed.filter(f.col("nullifiedScore")).select(
                        columns_to_report
                    ),
                    "evidenceNullifiedScoreDistinctFieldsCountByDatasource",
                ),
                # Evidence count by datasource (targets not resolved).
                evidence_distinct_fields_count(
                    evidence_failed.filter(~f.col("resolvedTarget")).select(
                        columns_to_report
                    ),
                    "evidenceUnresolvedTargetDistinctFieldsCountByDatasource",
                ),
                # Evidence count by datasource (diseases not resolved).
                evidence_distinct_fields_count(
                    evidence_failed.filter(~f.col("resolvedDisease")).select(
                        columns_to_report
                    ),
                    "evidenceUnresolvedDiseaseDistinctFieldsCountByDatasource",
                ),
            ]
        )

    AssociationsDataset = namedtuple("AssociationsDataset", "kind df filename")
    for associations in (
        AssociationsDataset(
            kind="Direct",
            df=associations_direct,
            filename=metrics_cfg.datasets.associations_source_direct,
        ),
        AssociationsDataset(
            kind="Indirect",
            df=associations_indirect,
            filename=metrics_cfg.datasets.associations_source_indirect,
        ),
        AssociationsDataset(
            kind="Direct",
            df=associations_overall_direct,
            filename=metrics_cfg.datasets.associations_overall_direct,
        ),
        AssociationsDataset(
            kind="Indirect",
            df=associations_overall_indirect,
            filename=metrics_cfg.datasets.associations_overall_indirect,
        ),
    ):
        if not associations.df:
            continue
        logging.info(f"Running metrics from {associations.filename}.")
        associations_df = associations.df
        if gold_standard:
            associations_df = associations_df.join(
                gold_standard, on=["targetId", "diseaseId"], how="left"
            ).fillna({"gold_standard": 0.0})
        if "Overall" not in associations.filename:
            datasets.extend(
                [
                    # Total association count.
                    document_total_count(
                        associations_df.select("diseaseId", "targetId").distinct(),
                        f"associations{associations.kind}TotalCount",
                    ),
                    # Associations by datasource.
                    document_count_by(
                        associations_df,
                        "datasourceId",
                        f"associations{associations.kind}ByDatasource",
                    ),
                    # Associations by datasource benchmark.
                    gold_standard_benchmark(
                        associations_df, f"{associations.kind}ByDatasource"
                    )
                    if gold_standard
                    else None,
                ]
            )
        else:
            datasets.extend(
                [
                    # Total association benchmark.
                    gold_standard_benchmark(
                        associations_df, f"{associations.kind}Overall"
                    )
                    if gold_standard
                    else None,
                ]
            )

    if diseases:
        logging.info(f"Running metrics from {metrics_cfg.datasets.diseases}.")
        datasets.extend(
            [
                document_total_count(diseases, "diseasesTotalCount"),
                not_null_fields_count(
                    diseases, "diseasesNotNullCount", group_by_datasource=False
                ),
            ]
        )

    if targets:
        logging.info(f"Running metrics from {metrics_cfg.datasets.targets}.")
        datasets.extend(
            [
                document_total_count(targets, "targetsTotalCount"),
            ]
        )

    if drugs:
        logging.info(f"Running metrics from {metrics_cfg.datasets.drugs}.")
        datasets.extend(
            [
                document_total_count(drugs, "drugsTotalCount"),
                not_null_fields_count(
                    drugs, "drugsNotNullCount", group_by_datasource=False
                ),
            ]
        )

    return datasets


@hydra.main(config_path=os.getcwd(), config_name="config")
def main(cfg: DictConfig) -> None:
    global spark
    spark = initialize_spark_session()

    logging_config = {
        "level": logging.INFO,
        "format": "%(name)s - %(levelname)s - %(message)s",
        "datefmt": "%Y-%m-%d %H:%M:%S",
    }
    logging.basicConfig(**logging_config)

    # Determine type of run and load evidence accordingly.
    cfg = cfg.metric_calculation
    ot_release = str(cfg.ot_release)
    if ot_release.endswith("_pre"):
        # Pre-ETL mode.
        is_pre_etl_run = True
        run_id = ot_release  # Example: "23.12_pre".
        logging.info(f"Fetching evidence for pre-ETL run {run_id}")
        evidence = fetch_pre_etl_evidence()
    else:
        # Post-ETL mode.
        is_pre_etl_run = False
        release_timestamp = detect_release_timestamp(cfg.metadata.evidence)
        if ot_release.startswith("partners/"):
            # Remove the "partners" prefix which was important for locating the files.
            ot_release = ot_release.split('/')[1] + "_ppp"
        # Calculate the final run ID:
        # - Example for regular: "23.12_2023-10-26".
        # - Example for PPP: "23.12_ppp_2023-11-27".
        run_id = f"{ot_release}_{release_timestamp}"
        logging.info(f"Reading evidence for post-ETL run {run_id}")
        evidence = read_path_if_provided(cfg.datasets.evidence)

    # Process evidence metrics.
    datasets = []
    if evidence:
        logging.info("Running evidence metrics.")
        columns_to_report = get_columns_to_report(evidence.columns)
        datasets.extend(
            [
                # Total evidence count.
                document_total_count(evidence, "evidenceTotalCount"),
                # Evidence count by datasource.
                document_count_by(
                    evidence, "datasourceId", "evidenceCountByDatasource"
                ),
                # Number of evidences that have a not null value in the given field.
                not_null_fields_count(
                    evidence,
                    "evidenceFieldNotNullCountByDatasource",
                    group_by_datasource=True,
                ),
                # distinctCount takes some time on all columns: subsetting them.
                evidence_distinct_fields_count(
                    evidence.select(columns_to_report),
                    "evidenceDistinctFieldsCountByDatasource",
                ),
            ]
        )

    # For the post-ETL mode, calculate lots of additional metrics from the output.
    if not is_pre_etl_run:
        datasets.extend(calculate_additional_post_etl_metrics(cfg))

    # Finalise the metrics dataframe.
    metrics = reduce(DataFrame.unionByName, datasets)
    metrics = metrics.withColumn("runId", f.lit(run_id)).cache()

    write_metrics_to_hf_hub(
        metrics,
        file_output_name=f"{run_id}.csv",
        repo_id=cfg.outputs.hf_repo_id,
    )
    logging.info(f"Metrics {run_id}.csv have been uploaded to HG Hub app: {cfg.outputs.hf_repo_id}")

    if not ot_release.endswith("_pre"):
        write_metrics_to_csv(metrics, cfg.outputs.release_output_path)
        logging.info(
            f"Wrote metrics to the release folder: {cfg.outputs.release_output_path}"
        )

    spark.stop()


if __name__ == "__main__":
    main()
