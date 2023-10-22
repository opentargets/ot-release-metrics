from __future__ import annotations

from typing import TYPE_CHECKING

import gcsfs
from pathlib import Path
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def get_cwd() -> str:
    """
    `get_cwd()` returns the current working directory as a string

    Returns:
      The current working directory.
    """
    return Path.cwd()


def initialize_spark_session():
    """
    It creates a SparkSession object, sets the master to YARN, and returns the SparkSession object

    Returns:
      A SparkSession object
    """
    return SparkSession.builder.master("yarn").getOrCreate()


def read_path_if_provided(path: str):
    """
    Automatically detect the format of the input data and read it into the Spark dataframe. The supported formats
    are: a single TSV file; a single JSON file; a directory with JSON files; a directory with Parquet files.
    """
    # All datasets are optional.
    if path is None:
        return None

    # The provided path must exist and must be either a file or a directory.
    assert gcsfs.GCSFileSystem().exists(
        path
    ), f"The provided path {path} does not exist."

    # Case 1: We are provided with a single file.
    if gcsfs.GCSFileSystem().isfile(path):
        if path.endswith(".tsv"):
            return SparkSession.getActiveSession().read.csv(path, sep="\t", header=True)
        elif path.endswith((".json", ".json.gz", ".jsonl", ".jsonl.gz", "json.bz2")):
            return SparkSession.getActiveSession().read.json(path)
        else:
            raise AssertionError(
                f"The format of the provided file {path} is not supported."
            )

    # Case 2: We are provided with a directory. We assume it contains parquet files.
    return SparkSession.getActiveSession().read.parquet(path)


def write_metrics_to_csv(metrics: DataFrame, output_path: str):
    """This function writes the dataframe to a csv file in the provided output folder."""

    # Write dataframe to csv file:
    metrics.toPandas().to_csv(output_path, index=False, header=True)
