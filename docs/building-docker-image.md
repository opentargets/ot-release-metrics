# Building a Docker image

```bash
# Modify tag as needed.
export TAG=latest
export IMAGE_NAME=ot-release-metrics
export PROJECT_NAME=open-targets-eu-dev
gcloud auth configure-docker
docker build -t ${IMAGE_NAME}:${TAG} .
docker tag ${IMAGE_NAME}:${TAG} gcr.io/${PROJECT_NAME}/${IMAGE_NAME}:${TAG}
docker push gcr.io/${PROJECT_NAME}/${IMAGE_NAME}:${TAG}
```
