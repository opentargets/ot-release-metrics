from __future__ import annotations

from typing import TYPE_CHECKING

from datasets import Dataset
import gcsfs
from huggingface_hub import login
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def access_gcp_secret(secret_id: str, project_id: str) -> str:
    """Access GCP secret manager to get the secret value.

    Args:
        secret_id (str): ID of the secret
        project_id (str): ID of the GCP project

    Returns:
        str: secret value
    """
    from google.cloud import secretmanager

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def initialize_spark_session():
    """Creates and returns a SparkSession object."""
    return SparkSession.builder.getOrCreate()


def read_path_if_provided(path: str | None, dir_format="parquet"):
    """
    Automatically detect the format of the input data and read it into the Spark dataframe. The supported formats
    are: a single TSV file; a single JSON file; a directory with JSON files; a directory with Parquet files.
    """
    # All datasets are optional.
    if path is None:
        return None

    # The provided path must exist and must be either a file or a directory.
    assert gcsfs.GCSFileSystem().exists(path), (
        f"The provided path {path} does not exist."
    )

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


def write_metrics_to_hf_hub(
    metrics_df: DataFrame,
    file_output_name: str,
    repo_id: str = "opentargets/ot-release-metrics",
    data_dir: str = "metrics",
    hf_token: str | None = None,
) -> None:
    """Upload dataset of metrics to HuggingFace Hub."""
    assert file_output_name.endswith(".csv"), "Only CSV files are supported."
    if hf_token:
        login(hf_token)
    hf_path = f"hf://datasets/{repo_id}/{data_dir}/{file_output_name}"
    ds = Dataset.from_pandas(metrics_df.toPandas())
    ds.to_csv(hf_path)
