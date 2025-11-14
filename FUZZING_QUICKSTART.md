# Quick Start: Fuzzing mssql-tds

## Prerequisites
```bash
rustup install nightly
cargo install cargo-fuzz
```

## Available Fuzz Targets

We have seven fuzz targets that test different parts of the TDS protocol:

1. **fuzz_token_stream**: Low-level token parsing
2. **fuzz_tds_client**: Query execution and result processing
3. **fuzz_connection_provider**: Connection establishment (original, mixed testing)
4. **fuzz_connection_provider_network**: Server response handling with fixed ClientContext
5. **fuzz_connection_provider_context**: ClientContext variations with minimal server responses
6. **fuzz_api_inputs**: RpcParameter API functions (get_sql_name, build_parameter_list)
7. **fuzz_parameter_encoding**: Parameter value encoding and serialization

## Run Fuzzing

### Fuzz Connection Provider (original - mixed testing)
```bash
cd /home/saurabh/work/mssql-tds/mssql-tds
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_connection_provider
```

### Fuzz Connection Provider Network (server response handling)
```bash
cd /home/saurabh/work/mssql-tds/mssql-tds
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_connection_provider_network
```

### Fuzz Connection Provider Context (client configuration)
```bash
cd /home/saurabh/work/mssql-tds/mssql-tds
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_connection_provider_context
```

### Fuzz TdsClient (query execution testing)
```bash
cd /home/saurabh/work/mssql-tds/mssql-tds
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_tds_client
```

### Fuzz API Inputs (RpcParameter functions)
```bash
cd /home/saurabh/work/mssql-tds/mssql-tds
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_api_inputs
```

### Fuzz Token Stream (low-level protocol)
```bash
cd /home/saurabh/work/mssql-tds/mssql-tds
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_token_stream
```

### Run for 1 Hour
```bash
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_tds_client -- -max_total_time=3600
```

### Run Overnight (8 hours)
```bash
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_tds_client -- -max_total_time=28800
```

## If Crashes are Found

### Reproduce a Crash
```bash
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_tds_client \
  fuzz/artifacts/fuzz_tds_client/crash-<hash>
```

### View Crash Details
```bash
ls -la fuzz/artifacts/fuzz_tds_client/
cat fuzz/artifacts/fuzz_tds_client/crash-<hash>
```

## Useful Commands

### List All Fuzz Targets
```bash
cargo fuzz list
```

### Check Coverage
```bash
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz coverage fuzz_tds_client
```

### Minimize Corpus (remove redundant test cases)
```bash
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz cmin fuzz_tds_client
```

### Add Custom Corpus Files
```bash
# Create binary file in corpus directory
echo -ne '\x00\xfd\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' > \
  fuzz/corpus/fuzz_tds_client/my_test_case
```

## What Each Scenario Tests

- **Scenario 0:** `execute()` - Sending queries and getting token response
- **Scenario 1:** `execute()` + `get_next_row()` - Full query-to-fetch flow
- **Scenario 2:** `move_to_column_metadata()` - Metadata parsing
- **Scenario 3:** `drain_stream()` - Stream exhaustion

The fuzzer will automatically try all scenarios with various token combinations.

## Expected Behavior

### Normal Operation
```
#1      NEW    cov: 42 ft: 43 corp: 1/14b exec/s: 0 rss: 32Mb
#2      NEW    cov: 45 ft: 48 corp: 2/28b exec/s: 0 rss: 32Mb
...
```

### If Crash Found
```
==12345==ERROR: libFuzzer: deadly signal
    #0 in TdsClient::execute ...
    ...
SUMMARY: libFuzzer: deadly signal
artifact_prefix='./'; Test unit written to ./crash-<hash>
```

## Stopping the Fuzzer
- Press `Ctrl+C` to stop
- Corpus is automatically saved

## Need Help?
See: `fuzz/FUZZING_TDS_CLIENT.md` for detailed documentation
