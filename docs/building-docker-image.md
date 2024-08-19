# Building a Docker image

Upload a image to Google's Artifact Registry.

## Metric calculation
Dockerise script that computes metrics for a given release.

```bash
# Froom root folder
export REPOSITORY=europe-west1-docker.pkg.dev/open-targets-eu-dev/ot-release-metrics
export IMAGE_NAME=metric-calculation
export TAG=latest # Modify tag as needed.
gcloud auth configure-docker
docker build -t ${REPOSITORY}/${IMAGE_NAME}:${TAG} -f metric-calculation/Dockerfile .
docker push ${REPOSITORY}/${IMAGE_NAME}:${TAG}
```

## Metric visualisation
Dockerise Streamlit app to visualise metrics.

```bash
# From root folder
export REPOSITORY=europe-west1-docker.pkg.dev/open-targets-eu-dev/ot-release-metrics
export IMAGE_NAME=app
export TAG=latest # Modify tag as needed.
gcloud auth configure-docker
docker build -t ${REPOSITORY}/${IMAGE_NAME}:${TAG} -f streamlit-app/Dockerfile .
docker push ${REPOSITORY}/${IMAGE_NAME}:${TAG}
```