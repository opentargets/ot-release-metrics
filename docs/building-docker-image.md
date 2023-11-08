# Building a Docker image

This repository is automated using GitHub Actions. On every branch push and on every release, a Docker image is built and uploaded to the Cloud Container Registry. Additionally, for the releases, the image is automatically tagged as “latest”.

This document contains commands which may need to be run in special cases.

## Building and uploading a Docker image manually
```bash
# Modify tag as needed.
export TAG=latest
export IMAGE_NAME=ot-release-metrics
export PROJECT_NAME=open-targets-eu-dev
docker build -t ${IMAGE_NAME}:${TAG}
gcloud auth configure-docker
docker tag ${IMAGE_NAME}:${TAG} gcr.io/${PROJECT_NAME}/${IMAGE_NAME}:${TAG}
docker push gcr.io/${PROJECT_NAME}/${IMAGE_NAME}:${TAG}
```

## Configuring a service account for the workflow
In case the workflow fails or needs to be modified in the future, this section contains the instructions on how to set it up from scratch. You need to have an “Owner” role in the project to be able to run these commands.

```bash
# Configure environment.
PROJECT_ID="open-targets-eu-dev"
SERVICE_ACCOUNT_NAME="ga-ot-release-metrics"

# Create the service account.
gcloud iam service-accounts create \
  $SERVICE_ACCOUNT_NAME \
  --project $PROJECT_ID

# Grant the necessary permissions to the service account.
for ROLE in storage.admin; do
  gcloud projects add-iam-policy-binding \
    $PROJECT_ID \
    --member=serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com --role=roles/$ROLE --condition="None"
done

# Create a JSON key for the service account and save it to a file.
gcloud iam service-accounts keys create /tmp/keyfile.json --iam-account=$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com
```

Next, copy the contents of the key from `/tmp/keyfile.json` and paste it as the `GCP_SA_KEY` secret variable in GitHub Actions.
