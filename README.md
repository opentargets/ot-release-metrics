# Open Targets release metrics calculation

Contains modules to calculate and visualise Open Targets release metrics.

👉 **Check it out here:**
https://share.streamlit.io/opentargets/ot-release-metrics/app.py

## Set up
This will create a Google Cloud instance, SSH into it and install the necessary dependencies. Tweak the commands as necessary.

```bash
# Set parameters.
export INSTANCE_NAME=evidence-metrics
export INSTANCE_ZONE=europe-west1-d

# Create the instance and SSH.
gcloud compute instances create \
  ${INSTANCE_NAME} \
  --project=open-targets-eu-dev \
  --zone=${INSTANCE_ZONE} \
  --machine-type=e2-highcpu-16 \
  --service-account=426265110888-compute@developer.gserviceaccount.com \
  --scopes=https://www.googleapis.com/auth/cloud-platform \
  --create-disk=auto-delete=yes,boot=yes,device-name=${INSTANCE_NAME},image=projects/ubuntu-os-cloud/global/images/ubuntu-2004-focal-v20210927,mode=rw,size=1000,type=projects/open-targets-eu-dev/zones/europe-west1-d/diskTypes/pd-balanced
gcloud compute ssh --zone ${INSTANCE_ZONE} ${INSTANCE_NAME}

# Set up the instance.
sudo apt update
sudo apt install -y \
  docker.io \
  openjdk-13-jre-headless \
  python3-pip python3-venv
git clone https://github.com/opentargets/ot-release-metrics
cd ot-release-metrics
python3 -m venv env
source env/bin/activate
python3 -m pip install -r requirements.txt
```

When logging to the same machine again or opening a new shell, you only need to activate the environment again:
```bash
cd ot-release-metrics
source env/bin/activate
```

## Run

### Pre-pipeline run
Before running, obtain a JSON credentials file for Google Cloud (someone from the backend team can generate one for you). Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to the location of this file.

The commands below will collect the latest evidence string JSON files for all sources. This is done using the [`platform-input-support`](https://github.com/opentargets/platform-input-support) module, which is being run through Docker.

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
export RUN_ID=21.11.1
time python3 metrics.py \
  --run-id ${RUN_ID}-pre \
  --out data/${RUN_ID}-pre.csv \
  --evidence platform-input-support/output/evidence-files/
```

Transfer the generated metrics file from the machine and commit it to the repository.

### Post-pipeline run
First specify the run identifier, ETL input and output roots. Note that there are two options for input/output roots depending on whether the script is being run on a completed release or on a snapshot (pick one option accordingly):
```bash
# For completed releases.
export ETL_RUN=21.09
export ETL_INPUT_ROOT=gs://open-targets-data-releases/21.09/input
export ETL_PARQUET_OUTPUT_ROOT=gs://open-targets-data-releases/21.09/output/etl/parquet

# For snapshots.
export ETL_RUN=21.09.2
export ETL_INPUT_ROOT=gs://open-targets-pre-data-releases/21.09.2/input
export ETL_PARQUET_OUTPUT_ROOT=gs://open-targets-pre-data-releases/21.09.2/output/etl/parquet
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
  ${ETL_INPUT_ROOT}/chembl-inputs/chembl_*molecule*.jsonl \
  post-pipeline
```

Next run the script to generate the metrics:
```bash
python3 metrics.py \
  --run-id ${ETL_RUN} \
  --out data/${ETL_RUN}.csv \
  --evidence post-pipeline/evidence \
  --evidence-failed post-pipeline/evidenceFailed \
  --associations-direct post-pipeline/associationByDatasourceDirect \
  --associations-indirect post-pipeline/associationByDatasourceIndirect \
  --diseases post-pipeline/diseases \
  --targets post-pipeline/targets \
  --drugs post-pipeline/*molecule*.jsonl \
  --gold-standard-associations gold-standard/informa_abbvie.tsv \
  --gold-standard-mappings gold-standard/mesh_mappings.tsv
```

## Running the web app to visualise and compare the metrics
To start the app locally, run `streamlit run app.py`.

If you encounter a `TomlDecodeError`, this can be resolved by removing the `~/.streamlit` folder.

## Update the web app
The application will be **automatically deployed** each time the `master` branch is updated.

The hosting is served using Streamlit's native Cloud service.

To make any changes, simply sign up at https://share.streamlit.io/signup with your personal GitHub account making sure access to the opentargets organisation repos is granted. You should be able to see and change the `ot-release-metrics` app configuration.
