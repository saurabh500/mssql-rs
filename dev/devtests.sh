#!/bin/bash
cargo nextest run -E "not (test(connectivity))" --all-targets -p tds-x --no-fail-fast --profile ci --success-output immediate
