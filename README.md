# Open Targets release metrics calculation

Contains modules to calculate and visualise Open Targets release metrics.

### Dependencies installation
```bash
python3 -m venv env
source env/bin/activate
python3 -m pip install -r requirements.txt
```

### Pre-pipeline run
You will first need to collect the latest evidence string JSON files for all sources. This can be done using the [`platform-input-support`](https://github.com/opentargets/platform-input-support) module. The easiest way is to run it is through Docker.

Before running, obtain a JSON credentials file for Google Cloud (someone from the backend team can generate one for you). Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to the location of this file.

```bash
export CREDENTIALS_PATH=$(realpath "${GOOGLE_APPLICATION_CREDENTIALS}")
export CREDENTIALS_DIR=$(dirname ${CREDENTIALS_PATH})
sudo docker run --rm \
  -v "${CREDENTIALS_DIR}":"${CREDENTIALS_DIR}" \
  -v "$(pwd)":"$(pwd)" \
  -e GOOGLE_APPLICATION_CREDENTIALS="${CREDENTIALS_PATH}" \
  -e GOOGLE_BUCKET=null/null \
  quay.io/opentargets/platform-input-support:master \
  --output_dir "$(pwd)/output" \
  -steps evidence
```

The process will fail when attempting to upload the files to the Google Cloud bucket, **which is expected** (we do not want to do that upload). A pull request to platform-input-support to make the upload step optional is pending.

The evidence strings will be collected in `output/evidence-files/` in the current working directory. Now the script is ready to be run:

```bash
source env/bin/activate
python3 metrics.py \
  --run-id test \
  --out metrics.csv \
  pre-pipeline \
  --evidence-json-dir output/evidence-files/
```

### Post-pipeline run
TODO.
