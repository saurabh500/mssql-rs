//! TLS helper utilities for mock server

use native_tls::Identity;
use std::fs;

/// Load a PKCS#12 identity from a file
pub fn load_identity_from_file(
    path: &str,
    password: &str,
) -> Result<Identity, Box<dyn std::error::Error>> {
    let bytes = fs::read(path)?;
    let identity = Identity::from_pkcs12(&bytes, password)?;
    Ok(identity)
}

/// Create a test PKCS#12 identity from PEM certificate and key
/// This is useful for testing purposes
///
/// On Windows, this function is not available because OpenSSL is not bundled.
/// Use `load_identity_from_file` with a pre-generated .pfx file instead.
#[cfg(not(windows))]
pub fn create_test_identity(
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<Identity, Box<dyn std::error::Error>> {
    // For simplicity, we'll use the openssl crate to create PKCS#12
    // In a real scenario, you'd use proper certificate management

    use openssl::nid::Nid;
    use openssl::pkcs12::Pkcs12;
    use openssl::pkey::PKey;
    use openssl::x509::X509;

    let cert = X509::from_pem(cert_pem)?;
    let key = PKey::private_key_from_pem(key_pem)?;

    // Use a non-empty password for macOS compatibility.
    // macOS Security.framework has issues importing PKCS12 with empty passwords.
    const PKCS12_PASSWORD: &str = "test";

    // Build PKCS12 using algorithms compatible with both OpenSSL 3.x and macOS.
    // - Use 3DES for both key and certificate encryption.
    // - RC2-40-CBC is not supported by OpenSSL 3.x (legacy algorithm).
    // - PBE_WITHSHA1AND3_KEY_TRIPLEDES_CBC is widely supported.
    let pkcs12 = Pkcs12::builder()
        .name("test")
        .pkey(&key)
        .cert(&cert)
        .key_algorithm(Nid::PBE_WITHSHA1AND3_KEY_TRIPLEDES_CBC)
        .cert_algorithm(Nid::PBE_WITHSHA1AND3_KEY_TRIPLEDES_CBC)
        .build2(PKCS12_PASSWORD)?;

    let der = pkcs12.to_der()?;
    let identity = Identity::from_pkcs12(&der, PKCS12_PASSWORD)?;

    Ok(identity)
}

/// On Windows, create_test_identity requires a pre-generated .pfx file
/// since OpenSSL is not bundled. This function loads from the standard test location.
#[cfg(windows)]
pub fn create_test_identity(
    _cert_pem: &[u8],
    _key_pem: &[u8],
) -> Result<Identity, Box<dyn std::error::Error>> {
    // On Windows, we don't have OpenSSL, so we load from the pre-generated .pfx file
    // The PEM arguments are ignored - the caller should have generated identity.pfx
    Err(
        "create_test_identity with PEM is not supported on Windows. \
         Use load_identity_from_file with a .pfx file instead. \
         Generate one using: .\\scripts\\generate_mock_tds_server_certs.ps1"
            .into(),
    )
}
