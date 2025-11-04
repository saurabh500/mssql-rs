# Fuzz Testing Strategy for PR and CI Pipelines

## Overview

This document outlines the strategy for integrating fuzz testing into the development workflow. The approach uses a tiered system to balance thoroughness with development velocity.

## Architecture

The fuzzing infrastructure is built on:

- cargo-fuzz with libFuzzer engine
- Azure Pipelines for CI/CD
- Linux-based build agents (fuzzing not supported on Windows MSVC)
- Nightly Rust toolchain

## Tiered Fuzzing Strategy

### Tier 1: Pull Request Checks

**Objective**: Detect obvious regressions without blocking development

**Configuration**:
- Duration: 60 seconds per fuzz target
- Workers: 1 parallel job
- Trigger: Every pull request
- Failure mode: Warning (non-blocking)

**Targets**:
- fuzz_done_token
- fuzz_token_stream

**Implementation**:
```yaml
steps:
- script: |
    rustup install nightly
    cargo +nightly install cargo-fuzz --version 0.11.4
  displayName: Install Fuzzing Tools

- script: |
    cd mssql-tds
    cargo +nightly fuzz run fuzz_done_token -- -max_total_time=60 -rss_limit_mb=2048
    cargo +nightly fuzz run fuzz_token_stream -- -max_total_time=60 -rss_limit_mb=2048
  displayName: Quick Fuzz Test
  continueOnError: true
  timeoutInMinutes: 5
```

**Rationale**:
- Provides fast feedback to developers
- Catches crashes introduced by recent changes
- Uses existing seed corpus for better coverage
- Does not slow down PR workflow

### Tier 2: Main Branch Integration

**Objective**: Thorough fuzzing after code is merged

**Configuration**:
- Duration: 30 minutes per fuzz target
- Workers: 4 parallel jobs
- Trigger: Commits to main branch
- Failure mode: Block build

**Additional Features**:
- Corpus minimization after fuzzing
- Artifact publishing for crashes
- Coverage statistics

**Implementation**:
```yaml
- script: |
    cd mssql-tds
    cargo +nightly fuzz run fuzz_done_token -- \
      -max_total_time=1800 \
      -jobs=4 \
      -rss_limit_mb=8192 \
      -print_final_stats=1
  displayName: Fuzz DoneToken
  timeoutInMinutes: 35

- script: |
    cd mssql-tds
    cargo +nightly fuzz cmin fuzz_done_token
    cargo +nightly fuzz cmin fuzz_token_stream
  displayName: Minimize Corpus
```

**Rationale**:
- Provides deeper testing without affecting PR velocity
- Builds enhanced corpus over time
- Catches bugs that require more iterations to find

### Tier 3: Nightly Fuzzing

**Objective**: Find rare bugs requiring extended fuzzing

**Configuration**:
- Duration: 8 hours per fuzz target
- Workers: 8 parallel jobs
- Trigger: Daily at 2:00 AM
- Failure mode: Alert team

**Additional Features**:
- Multiple sanitizers (AddressSanitizer, MemorySanitizer)
- Coverage report generation
- Corpus statistics
- Crash deduplication

**Implementation**:
```yaml
schedules:
- cron: "0 2 * * *"
  displayName: Nightly Fuzz Testing
  branches:
    include:
    - main

steps:
- script: |
    cd mssql-tds
    cargo +nightly fuzz run fuzz_done_token -- \
      -max_total_time=28800 \
      -jobs=8 \
      -rss_limit_mb=16384 \
      -print_final_stats=1 \
      -print_corpus_stats=1
  displayName: Deep Fuzz - DoneToken
```

**Rationale**:
- Explores edge cases requiring many iterations
- Does not consume resources during working hours
- Provides comprehensive security testing

## Corpus Management

### Storage Strategy

The fuzzing corpus grows over time and needs persistent storage:

**Option 1: Azure Storage**
```yaml
- script: |
    az storage blob download-batch \
      --account-name fuzzartifacts \
      --source fuzz-corpus \
      --destination mssql-tds/fuzz/corpus
  displayName: Restore Previous Corpus

- script: |
    az storage blob upload-batch \
      --account-name fuzzartifacts \
      --destination fuzz-corpus \
      --source mssql-tds/fuzz/corpus
  displayName: Upload Enhanced Corpus
```

**Option 2: Git LFS**
- Store corpus in repository with Git LFS
- Automatically versioned with code
- Easier to track corpus evolution

**Option 3: Pipeline Artifacts**
```yaml
- task: PublishPipelineArtifact@1
  displayName: Publish Fuzz Corpus
  inputs:
    targetPath: mssql-tds/fuzz/corpus
    artifact: FuzzCorpus-$(Build.BuildNumber)
```

### Corpus Minimization

Regularly minimize the corpus to remove redundant test cases:

```bash
cargo +nightly fuzz cmin fuzz_done_token
```

This reduces:
- Storage requirements
- Fuzzing startup time
- Redundant test case execution

## Crash Handling

### Detection

After each fuzzing run, check for crashes:

```yaml
- script: |
    if [ -d "mssql-tds/fuzz/artifacts" ] && [ "$(ls -A mssql-tds/fuzz/artifacts)" ]; then
      echo "Fuzzing found crashes"
      find mssql-tds/fuzz/artifacts -type f
      exit 1
    fi
  displayName: Check for Crashes
```

### Artifact Collection

Publish crash files for analysis:

```yaml
- task: PublishPipelineArtifact@1
  displayName: Publish Crash Artifacts
  condition: failed()
  inputs:
    targetPath: mssql-tds/fuzz/artifacts
    artifact: FuzzCrashes-$(Build.BuildNumber)
```

### Minimization

Minimize crash test cases before analysis:

```bash
cargo +nightly fuzz tmin fuzz_done_token artifacts/fuzz_done_token/crash-abc123
```

This produces the smallest input that reproduces the crash.

### Reproduction

Reproduce crashes locally:

```bash
cargo +nightly fuzz run fuzz_done_token artifacts/fuzz_done_token/crash-abc123
```

## Sanitizer Integration

Run fuzzing with different sanitizers to catch various bug classes:

### AddressSanitizer

Detects:
- Buffer overflows
- Use-after-free
- Double-free
- Memory leaks

```bash
RUSTFLAGS="-Z sanitizer=address" \
  cargo +nightly fuzz run fuzz_done_token -- -max_total_time=300
```

### MemorySanitizer

Detects:
- Use of uninitialized memory

```bash
RUSTFLAGS="-Z sanitizer=memory" \
  cargo +nightly fuzz run fuzz_done_token -- -max_total_time=300
```

### UndefinedBehaviorSanitizer

Detects:
- Integer overflow
- Null pointer dereference
- Invalid type conversions

```bash
RUSTFLAGS="-Z sanitizer=undefined" \
  cargo +nightly fuzz run fuzz_done_token -- -max_total_time=300
```

## Performance Monitoring

Track fuzzing effectiveness with metrics:

### Execution Speed

```
exec/s: Number of test cases executed per second
```

Target: Above 1000 exec/s indicates good performance

### Coverage Growth

```
cov: Number of unique code edges covered
```

Monitor coverage growth over time to ensure fuzzing is discovering new paths.

### Corpus Growth

```
corp: Number of interesting inputs in corpus
```

Corpus should grow initially, then stabilize as coverage saturates.

### Crash Rate

```
crashes per 100,000 executions
```

Track crash rate to identify unstable code areas.

## Reporting

Generate reports after each fuzzing run:

```yaml
- script: |
    cat > fuzz_report.md << 'EOF'
    # Fuzzing Report
    
    Date: $(date)
    Duration: 30 minutes per target
    Workers: 4
    
    ## Coverage
    $(cargo +nightly fuzz coverage fuzz_done_token 2>&1 | grep "coverage")
    
    ## Crashes Found
    $(find fuzz/artifacts -type f | wc -l)
    
    ## Crash Details
    $(find fuzz/artifacts -type f)
    EOF
  displayName: Generate Report
```

## Integration Steps

### Phase 1: PR Integration

1. Create fuzz-pr-template.yml in .pipeline/templates/
2. Add Quick_Fuzz job to validation-pipeline.yml
3. Configure as non-blocking warning
4. Monitor for false positives

### Phase 2: Main Branch Integration

1. Create fuzz-ci-template.yml
2. Add Fuzz_CI stage to validation-pipeline.yml
3. Set up corpus storage (Azure Storage or artifacts)
4. Configure as blocking check

### Phase 3: Nightly Fuzzing

1. Create fuzz-nightly.yml pipeline
2. Configure scheduled trigger
3. Set up alerting for failures
4. Establish review process for findings

### Phase 4: Continuous Improvement

1. Add new fuzz targets for complex parsers
2. Expand corpus with interesting test cases
3. Tune performance parameters
4. Monitor and optimize coverage growth

## Resource Requirements

### Compute Resources

**PR Checks**:
- 1 Linux build agent
- 2-3 minutes total runtime
- 2 GB RAM per worker

**Main Branch**:
- 1 Linux build agent with 4+ cores
- 30-40 minutes total runtime
- 8 GB RAM per worker

**Nightly**:
- 1 Linux build agent with 8+ cores
- 8-9 hours total runtime
- 16 GB RAM per worker

### Storage Requirements

**Corpus**:
- Initial: 1-10 MB
- Growth: 10-50 MB per month
- Maximum: 500 MB (with minimization)

**Artifacts**:
- Per crash: 1-100 KB
- Retention: 90 days
- Estimated: 1-5 GB per year

## Continuous Fuzzing Options

For projects requiring dedicated fuzzing infrastructure:

### OSS-Fuzz Integration

If the project is open source, integrate with OSS-Fuzz:

**Benefits**:
- Free Google Cloud infrastructure
- Automatic crash reporting
- ClusterFuzz integration
- Coverage tracking dashboard
- Regression testing

**Requirements**:
- Project must be open source
- Integration configuration file
- Maintain build.sh and Dockerfile

### Self-Hosted Continuous Fuzzing

For private projects, set up dedicated fuzzing infrastructure:

**Components**:
- Dedicated Linux VMs or containers
- Corpus synchronization system
- Crash deduplication service
- Dashboard for metrics
- Alert system for new crashes

**Implementation**:
```yaml
- job: Continuous_Fuzz
  pool: DedicatedFuzzPool
  strategy:
    parallel: 8
  steps:
    - script: |
        while true; do
          cargo +nightly fuzz run fuzz_done_token -- -max_total_time=3600
          sync_corpus_to_storage
          sleep 60
        done
```

## Best Practices

### Seed Corpus

Maintain a good seed corpus:
- Valid protocol messages
- Edge cases from bug reports
- Manually crafted corner cases
- Minimized versions of interesting crashes

### Dictionary Files

Create dictionary files with protocol constants:

```
# fuzz/fuzz.dict
token_done="\xFD"
token_doneproc="\xFE"
token_doneinproc="\xFF"
status_final="\x00\x00"
status_more="\x01\x00"
cmd_select="\xc1\x00"
```

Use with:
```bash
cargo +nightly fuzz run fuzz_done_token -- -dict=fuzz/fuzz.dict
```

### Parallel Fuzzing

Run multiple workers to explore input space faster:

```bash
cargo +nightly fuzz run fuzz_done_token -- -jobs=8 -workers=8
```

### Memory Limits

Set appropriate memory limits to prevent OOM:

```bash
cargo +nightly fuzz run fuzz_done_token -- -rss_limit_mb=2048
```

### Timeout Detection

Enable timeout detection for hang detection:

```bash
cargo +nightly fuzz run fuzz_done_token -- -timeout=10
```

## Maintenance

### Regular Tasks

**Weekly**:
- Review crash reports
- Update seed corpus with interesting cases
- Monitor coverage metrics

**Monthly**:
- Minimize corpus
- Review and tune fuzzing parameters
- Add new fuzz targets for new code

**Quarterly**:
- Evaluate fuzzing effectiveness
- Update tooling (cargo-fuzz, LLVM)
- Review and update this strategy

### Troubleshooting

**Slow Execution Speed**:
- Reduce input size with -max_len parameter
- Profile target code for bottlenecks
- Disable expensive sanitizers for initial runs

**Out of Memory**:
- Reduce -rss_limit_mb parameter
- Decrease number of workers
- Clear corpus and restart

**Low Coverage Growth**:
- Improve seed corpus quality
- Add dictionary files
- Review target code structure
- Consider structural fuzzing with arbitrary crate

## Metrics and Success Criteria

### Coverage Targets

- Week 1: Establish baseline coverage
- Month 1: 10% improvement over baseline
- Month 3: 25% improvement over baseline
- Month 6: 40% improvement over baseline

### Performance Targets

- Execution speed: Above 1000 exec/s
- Fuzzing uptime: Above 95% for nightly runs
- Crash response time: Analysis within 24 hours

### Quality Targets

- Zero crashes from fuzzing found in production
- All crashes minimized and added to regression tests
- Corpus coverage stable for 30 days indicates saturation

## Conclusion

This tiered fuzzing strategy provides:

- Fast feedback during development (PR checks)
- Thorough testing after integration (main branch)
- Deep exploration of edge cases (nightly runs)
- Minimal impact on development velocity
- Scalable resource usage

The strategy can be expanded over time by adding more fuzz targets, increasing fuzzing duration, or integrating continuous fuzzing infrastructure.
