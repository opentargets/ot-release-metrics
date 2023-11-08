# Building a Docker image

This repository is automated using GitHub Actions. On every branch push and on every release, a Docker image is built and uploaded to the Cloud Container Registry.

However, in case the workflow fails or needs to be modified in the future, this document contains the instructions on how to set it up from scratch. You need to have an “Owner” role in the project to be able to run these commands.

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
