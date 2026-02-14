#!/bin/bash

export ACR_NAME="tdslibrs"
# Login to az acr
az acr login -n $ACR_NAME

# List of images to import
IMAGES=(
  "alpine:3.18"
    "alpine:3.19"
    "alpine:3.20"
    "alpine:3.21"
    "debian:bookworm"
    "redhat/ubi9:latest"
    "oraclelinux:9"
    "ubuntu:22.04"
    "ubuntu:24.04"
)

for IMAGE_NAME in "${IMAGES[@]}"; do
  echo "Importing image $IMAGE_NAME into ACR $ACR_NAME..."
  docker tag "$IMAGE_NAME" "$ACR_NAME.azurecr.io/$IMAGE_NAME"
    docker push "$ACR_NAME.azurecr.io/$IMAGE_NAME"
done