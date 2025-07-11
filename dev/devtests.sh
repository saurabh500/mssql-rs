#!/bin/bash
cargo nextest run -E "not (test(connectivity))" --all-targets -p mssql-tds --no-fail-fast --profile ci --success-output immediate
