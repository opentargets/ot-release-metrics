from __future__ import annotations

from typing import TYPE_CHECKING
import yaml

from addict import Dict
import gcsfs
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from modules.common.Downloads import Downloads
from modules.common.GoogleBucketResource import GoogleBucketResource

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def initialize_spark_session():
    """Creates and returns a SparkSession object."""
    return SparkSession.builder.getOrCreate()


def read_path_if_provided(path: str, dir_format="parquet"):
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

    # Case 2: We are provided with a directory.
    read_method = SparkSession.getActiveSession().read
    if dir_format == "parquet":
        read_method = read_method.parquet
    elif dir_format == "json":
        read_method = read_method.json
    else:
        raise NotImplementedError(
            "Only Parquet or JSON are supported for partitioned files."
        )
    return read_method(path)


def write_metrics_to_csv(metrics: DataFrame, output_path: str):
    """This function writes the dataframe to a csv file in the provided output folder."""

    # Write dataframe to csv file:
    metrics.toPandas().to_csv(output_path, index=False, header=True)


def detect_release_timestamp(evidence_metadata_path):
    """Automatically detects ETL run timestamp based on the latest update time."""
    evidence_metadata = read_path_if_provided(evidence_metadata_path, format="json")
    parquet_rows = evidence_metadata.filter(
        f.col("resource").getField("format") == "parquet"
    ).collect()
    assert (
        len(parquet_rows) == 1
    ), "There should be exactly one row with Parquet metadata"
    update_timestamp = parquet_rows[
        0
    ].timeStamp  # Example format: "2023-10-31T18:11:57.508Z".
    update_date = update_timestamp[:10]  # Example format: "2023-10-31".
    return update_date


def fetch_pre_etl_evidence():
    """Returns latest evidence for each input source, fetched using PIS code."""
    pis_config = Dict(
        yaml.load(
            open("/home/spark/platform-input-support/config.yaml").read(),
            yaml.SafeLoader,
        )
    )

    # Instantiate a helper class to identify the latest file in each bucket.
    pis_downloads = Downloads(None)

    # Identify latest files for each evidence source.
    all_files = []
    for resource in pis_config.evidence.gs_downloads_latest:
        bucket_name, path = GoogleBucketResource.get_bucket_and_path(resource.bucket)
        google_resource = GoogleBucketResource(bucket_name, path)
        latest_resource = pis_downloads.get_latest(google_resource, resource)
        latest_filename = latest_resource["latest_filename"]
        all_files.append(f"gs://{bucket_name}/{latest_filename}")

    # Read all files into Spark.
    return SparkSession.getActiveSession().read.json(all_files)
