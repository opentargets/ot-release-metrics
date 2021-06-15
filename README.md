# Open Targets release metrics calculation

Contains modules to calculate and visualise Open Targets release metrics.

## Installation
As system level dependencies, the modules require Python3 with the `venv` module and Java. In Ubuntu, they can be installed using `sudo apt install -y python3-venv openjdk-14-jre-headless`.

After that, create and prepare the environment:
```bash
python3 -m venv env
source env/bin/activate
python3 -m pip install -r requirements.txt
```

## Running

The modules can be run locally or in the Google Cloud (in this case, a `e2-highcpu-8` instance is recommended).

### Pre-pipeline run
You will first need to collect the latest evidence string JSON files for all sources. This can be done using the [`platform-input-support`](https://github.com/opentargets/platform-input-support) module. The easiest way is to run it is through Docker.

Before running, obtain a JSON credentials file for Google Cloud (someone from the backend team can generate one for you). Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to the location of this file.

```bash
# I recommend that you *do not* attempt to simplify the commands below, as the way PIS writes the output files can be a
# bit tricky. For example, the reason we still have to clone the PIS repository, even though running via Docker, is
# because it will only write the files to the 'output/' directory relative to its code root; and to properly map this we
# need to have a local copy.
export CREDENTIALS_PATH=$(realpath "${GOOGLE_APPLICATION_CREDENTIALS}")
export CREDENTIALS_DIR=$(dirname ${CREDENTIALS_PATH})
git clone -q https://github.com/opentargets/platform-input-support
mkdir platform-input-support/output
sudo docker run --rm \
  -v "${CREDENTIALS_DIR}":"${CREDENTIALS_DIR}" \
  -v $(realpath platform-input-support):/usr/src/app \
  -e PIS_CONFIG=/usr/src/app/config.yaml \
  -e GOOGLE_APPLICATION_CREDENTIALS="${CREDENTIALS_PATH}" \
  -e GOOGLE_BUCKET=null/null \
  quay.io/opentargets/platform-input-support:master \
  -steps evidence
```

The process will fail when attempting to upload the files to the Google Cloud bucket, **which is expected** (we do not want to do that upload). A pull request to platform-input-support to make the upload step optional is pending.

The evidence strings will be collected in `platform-input-support/output/evidence-files/` relative to the current working directory. Now the script is ready to be run:

```bash
source env/bin/activate
python3 metrics.py \
  --run-id 21.02.2-pre \
  --out data/21.02.2-pre.csv \
  --evidence platform-input-support/output/evidence-files/
```

### Post-pipeline run
First specify the ETL input and output roots. Note that there are two options for the output root depending on whether the script is being run on a completed release or on a snapshot (pick one option accordingly):
```bash
export ETL_PARQUET_OUTPUT_ROOT=gs://ot-snapshots/etl/outputs/21.04.2/parquet  # For snapshots.
export ETL_PARQUET_OUTPUT_ROOT=gs://open-targets-data-releases/21.04/output/etl/parquet  # For completed releases.
export ETL_INPUT_ROOT=gs://open-targets-data-releases/21.04/input
```

Now download the files:
```bash
mkdir post-pipeline
gsutil -m cp -r \
  ${ETL_PARQUET_OUTPUT_ROOT}/evidence \
  ${ETL_PARQUET_OUTPUT_ROOT}/evidenceFailed \
  ${ETL_PARQUET_OUTPUT_ROOT}/associationByDatasourceDirect \
  ${ETL_PARQUET_OUTPUT_ROOT}/associationByDatasourceIndirect \
  ${ETL_PARQUET_OUTPUT_ROOT}/diseases \
  ${ETL_PARQUET_OUTPUT_ROOT}/targets \
  ${ETL_INPUT_ROOT}/annotation-files/chembl/chembl_*molecule*.jsonl \
  post-pipeline
```

Next run the script to generate the metrics:
```bash
source env/bin/activate
python3 metrics.py \
  --run-id 21.02.2-post \
  --out data/21.02.2-post.csv \
  --evidence post-pipeline/evidence \
  --evidence-failed post-pipeline/evidenceFailed \
  --associations-direct post-pipeline/associationByDatasourceDirect \
  --associations-indirect post-pipeline/associationByDatasourceIndirect \
  --diseases post-pipeline/diseases \
  --targets post-pipeline/targets \
  --drugs post-pipeline/molecule
```