# ServerCertificate Test Summary

This document provides an overview of all tests implemented for the ServerCertificate feature.

## Test Coverage Overview

The ServerCertificate feature has comprehensive test coverage across two test categories:

### 1. **Unit Tests** (12 tests)
Location: `mssql-tds/src/connection/transport/certificate_validator.rs`

These tests validate the core certificate validation logic:

#### Constant-Time Comparison Tests (6 tests)
- **test_constant_time_compare_equal**: Validates that identical byte slices compare as equal
- **test_constant_time_compare_different**: Validates that different byte slices compare as not equal
- **test_constant_time_compare_different_sizes**: Validates that slices of different sizes compare as not equal
- **test_constant_time_compare_all_zeros**: Tests comparison of all-zero slices
- **test_constant_time_compare_single_bit_difference**: Tests detection of single bit differences at various positions
- **test_constant_time_compare_empty_slices**: Tests comparison of empty slices

#### Certificate Loading Tests (6 tests)
- **test_load_certificate_file_not_found**: Validates proper error when certificate file doesn't exist
- **test_load_certificate_from_pem**: Tests loading a valid PEM-formatted certificate
- **test_load_certificate_from_der**: Tests loading a valid DER-formatted certificate
- **test_load_certificate_invalid_format**: Tests error handling for invalid certificate files
- **test_pem_and_der_certificates_produce_same_der**: Validates that PEM and DER files of the same certificate produce identical DER encodings
- **test_is_certificate_expired_valid**: Tests certificate expiry validation for a valid certificate

### 2. **Integration Tests** (3 tests)
Location: `mssql-tds/tests/test_mock_server.rs`

These tests validate the ServerCertificate feature in end-to-end scenarios:

#### Configuration and Error Handling Tests
- **test_server_certificate_invalid_path**: 
  - Tests behavior when ServerCertificate points to a non-existent file
  - Validates that invalid paths don't cause crashes
  - Uses PreferOff encryption mode since mock server doesn't support TLS

- **test_server_certificate_with_trust_server_certificate**:
  - Tests precedence rules when both ServerCertificate and TrustServerCertificate are set
  - Validates that ServerCertificate takes precedence (as per ODBC behavior)
  - Creates a temporary certificate file for testing

- **test_server_certificate_with_hostname_in_cert_fails**:
  - Tests mutual exclusivity of ServerCertificate and HostnameInCertificate options
  - Validates configuration handling when both options are specified
  - Uses PreferOff encryption mode to test configuration acceptance

## Test Certificates

Test certificates are located in `mssql-tds/tests/test_certificates/`:

- **valid_cert.pem**: Self-signed certificate in PEM format (valid for 10 years)
- **valid_cert.der**: Same certificate in DER format
- **invalid_format.txt**: Invalid file for testing error handling
- **key.pem**: Private key (not used in validation tests)
- **README.md**: Documentation on regenerating test certificates

## Running the Tests

### Run All Certificate Validator Tests
```bash
cargo test --package mssql-tds certificate_validator
```

### Run ServerCertificate Integration Tests
```bash
cargo test --package mssql-tds test_server_certificate
```

### Run All Tests
```bash
cargo test --package mssql-tds
```

## Test Results

**All 15 tests pass successfully:**
- ✅ 12 unit tests in certificate_validator module
- ✅ 3 integration tests in test_mock_server

## Limitations and Future Work

### Current Limitations
1. **Mock Server No TLS Support**: The mock TDS server (`mssql-mock-tds`) doesn't implement TLS/SSL, so integration tests use `EncryptionSetting::PreferOff` mode. This limits testing of the actual TLS certificate validation flow.

2. **Manual Testing Required**: Complete end-to-end testing of ServerCertificate with TLS requires connecting to a real SQL Server instance.

### Future Enhancements
1. **Add TLS Support to Mock Server**: Enhance `mssql-mock-tds` to support TLS so integration tests can validate the complete certificate pinning flow.

2. **Add Tests for Certificate Mismatch**: Create tests that verify detection of mismatched certificates during TLS handshake.

3. **Add Tests for Expired Certificates**: Create expired test certificates to validate expiry detection.

4. **Add Performance Tests**: Validate that constant-time comparison has consistent timing regardless of input.

## Security Testing Notes

The constant-time comparison function is critical for security:
- Tests validate correct comparison logic across various scenarios
- Single-bit differences are detected at all positions (start, middle, end)
- Empty slices and all-zero slices are handled correctly
- Different-sized inputs are properly detected

## Test Maintenance

When updating the certificate validator:
1. Run all tests: `cargo test --package mssql-tds certificate_validator`
2. Verify constant-time comparison behavior is preserved
3. Update test certificates if they expire (every 10 years)
4. Add new tests for any new error cases or validation rules
