# Cross-Repo Development: mssql-tds + mssql-python

The mssql-tds pipeline can test Rust changes against the full mssql_python driver.

## Test Structure

```
mssql-py-core/tests/
├── conftest.py              # mssql_py_core fixtures
├── test_bulkcopy_int.py     # Direct Rust binding tests
├── test_connection.py
│
└── mssql_python/            # Full driver tests
    ├── conftest.py          # `from mssql_python import connect`
    └── test_bulkcopy.py
```

| Directory | What it tests | Runs |
|-----------|--------------|------|
| `tests/` | Rust bindings directly | Every PR |
| `tests/mssql_python/` | Full Python driver | When `enableMssqlPythonTest: true` |

## Enable in Pipeline

```yaml
- template: templates/build-template-container.yml
  parameters:
    enableMssqlPythonTest: true
```

## Local Development

```bash
./dev/test-mssql-python-integration.sh
# or with --clone to fetch mssql-python
```
