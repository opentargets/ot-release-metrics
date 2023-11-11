from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

import gcsfs
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def initialize_spark_session():
    """Creates and returns a SparkSession object."""
    return SparkSession.builder.getOrCreate()


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


def fetch_pre_etl_evidence():
    """Fetch pre-ETL evidence using platform-input-support bundled inside the Docker container.

    First, the evidence is fetched locally, and then it is ingested into the Hadoop HDFS, where it can be picked up by Spark.
    """
    cmd = (
        "python3 /platform-input-support/platform-input-support.py -steps Evidence -o $HOME/output &> $HOME/pis.log && "
        "hadoop fs -copyFromLocal $HOME/output/prod/evidence-files /"
    )
    process = subprocess.Popen(cmd, shell=True)
    process.wait()
    assert (process.returncode == 0, "Fetching pre-ETL evidence using PIS failed")


def detect_release_timestamp(evidence_path):
    """Automatically detects ETL run timestamp based on the latest update time."""
    evidence_success_marker = f"{evidence_path}/_SUCCESS"
    evidence_metadata = gcsfs.GCSFileSystem().info(evidence_success_marker)
    update_time = evidence_metadata['updated']
    iso_timestamp = update_time.strftime('%Y-%m-%d')
    return iso_timestamp
