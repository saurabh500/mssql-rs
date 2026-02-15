# Smoke-test image for mssql-py-core private link testing
# Built by CI — receives the .whl and tests/ directory at build time.
# Base image mirrored from docker.io/library/python:3.14-slim into our ACR.

FROM tdslibrs.azurecr.io/import/python:3.14-slim

RUN apt-get update && apt-get install -y --no-install-recommends libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY *.whl ./
RUN pip install --no-cache-dir *.whl pytest python-dotenv azure-identity

COPY tests/ tests/
COPY pytest.ini .

RUN mkdir -p /results

CMD ["pytest", "tests/smoke/", "-v", "--tb=short", "--junitxml=/results/smoke-results.xml"]
