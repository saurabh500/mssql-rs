#!/bin/bash

set -ex
set -o pipefail
echo "az acr login -n tdslibrs needs to be run first, in case it is not already logged in"
pwd
docker build -f Dockerfile.Alpine.Build -t tdslibrs.azurecr.io/build/x64/alpine:3.18 .
docker push tdslibrs.azurecr.io/build/x64/alpine:3.18

docker build -f Dockerfile.Ubuntu.Build -t tdslibrs.azurecr.io/build/x64/ubuntu-build:22.04 .
docker push tdslibrs.azurecr.io/build/x64/ubuntu-build:22.04
