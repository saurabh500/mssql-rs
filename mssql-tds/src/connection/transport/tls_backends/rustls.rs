// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! `rustls` implementation of [`TlsBackend`].

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

use async_trait::async_trait;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error as RustlsError, SignatureScheme};
use tokio_rustls::client::TlsStream;
use tracing::{debug, info};

use crate::connection::transport::network_transport::Stream;
use crate::connection::transport::tls_backend::{
    TlsBackend, TlsHandshakeOutcome, TlsValidationConfig,
};
use crate::core::{TDS_8_ALPN_PROTOCOL, TdsResult};
use crate::error::Error;

static CONFIG_CACHE: LazyLock<RwLock<HashMap<TlsValidationConfig, Arc<ClientConfig>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

#[derive(Debug)]
struct AcceptAllCertVerifier;

impl ServerCertVerifier for AcceptAllCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}

#[derive(Debug)]
struct SkipHostnameVerifier {
    inner: Arc<dyn ServerCertVerifier>,
}

impl ServerCertVerifier for SkipHostnameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        let dummy_name = ServerName::try_from("hostname.verification.disabled")
            .map_err(|e| RustlsError::General(format!("invalid dummy DNS name: {e}")))?;

        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            &dummy_name,
            ocsp_response,
            now,
        ) {
            Ok(verified) => Ok(verified),
            Err(RustlsError::InvalidCertificate(rustls::CertificateError::NotValidForName)) => {
                Ok(ServerCertVerified::assertion())
            }
            Err(e) => Err(e),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

fn get_or_build_config(validation: &TlsValidationConfig) -> TdsResult<Arc<ClientConfig>> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    if let Some(config) = CONFIG_CACHE
        .read()
        .map_err(|_| Error::ImplementationError("TLS config cache read lock poisoned".to_string()))?
        .get(validation)
    {
        return Ok(Arc::clone(config));
    }

    let config = Arc::new(build_client_config(validation)?);
    CONFIG_CACHE
        .write()
        .map_err(|_| {
            Error::ImplementationError("TLS config cache write lock poisoned".to_string())
        })?
        .insert(validation.clone(), Arc::clone(&config));

    Ok(config)
}

fn build_root_store() -> rustls::RootCertStore {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let native_certs = rustls_native_certs::load_native_certs();
    if !native_certs.errors.is_empty() {
        debug!(
            "Errors loading some native certs (using webpki-roots as fallback): {:?}",
            native_certs.errors
        );
    }
    for cert in native_certs.certs {
        if let Err(e) = root_store.add(cert) {
            debug!("Skipping a native cert that couldn't be added: {e}");
        }
    }

    root_store
}

fn build_client_config(validation: &TlsValidationConfig) -> TdsResult<ClientConfig> {
    let mut config = if validation.accept_invalid_certs {
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAllCertVerifier))
            .with_no_client_auth()
    } else if validation.accept_invalid_hostnames {
        let verifier = WebPkiServerVerifier::builder(Arc::new(build_root_store()))
            .build()
            .map_err(|e| {
                Error::ImplementationError(format!("failed to build rustls verifier: {e}"))
            })?;
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SkipHostnameVerifier { inner: verifier }))
            .with_no_client_auth()
    } else {
        ClientConfig::builder()
            .with_root_certificates(build_root_store())
            .with_no_client_auth()
    };

    if validation.use_alpn {
        config.alpn_protocols = vec![TDS_8_ALPN_PROTOCOL.as_bytes().to_vec()];
    }

    Ok(config)
}

/// `rustls` backend.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct RustlsBackend;

#[async_trait]
impl TlsBackend for RustlsBackend {
    async fn perform_handshake(
        &self,
        validation: &TlsValidationConfig,
        host_name: &str,
        base_stream: Box<dyn Stream>,
    ) -> TdsResult<TlsHandshakeOutcome> {
        let config = get_or_build_config(validation)?;
        let server_name = ServerName::try_from(host_name.to_string()).map_err(|e| {
            Error::ConnectionError(format!("Invalid TLS server name '{}': {}", host_name, e))
        })?;

        info!("Starting rustls handshake using host {}", host_name);
        let tls_stream = tokio_rustls::TlsConnector::from(config)
            .connect(server_name, base_stream)
            .await
            .map_err(|e| Error::RustlsHandshakeError {
                source: e,
                expected_host: host_name.to_string(),
                cert_sans: "(unavailable - handshake failed before certificate could be retrieved)"
                    .to_string(),
            })?;

        let negotiated_alpn = if validation.use_alpn {
            tls_stream.get_ref().1.alpn_protocol().map(Vec::from)
        } else {
            None
        };

        let peer_cert_der = tls_stream
            .get_ref()
            .1
            .peer_certificates()
            .and_then(|certs| certs.first().map(|cert| cert.as_ref().to_vec()));

        Ok(TlsHandshakeOutcome {
            stream: Box::new(tls_stream),
            negotiated_alpn,
            peer_cert_der,
        })
    }
}

impl Stream for TlsStream<Box<dyn Stream>> {
    fn tls_handshake_starting(&mut self) {
        self.get_mut().0.tls_handshake_starting();
    }

    fn tls_handshake_completed(&mut self) {
        self.get_mut().0.tls_handshake_completed();
    }

    fn is_connection_dead(&self) -> bool {
        self.get_ref().0.is_connection_dead()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn ensure_crypto_provider() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    fn validation(use_alpn: bool) -> TlsValidationConfig {
        TlsValidationConfig {
            accept_invalid_certs: true,
            accept_invalid_hostnames: false,
            use_alpn,
        }
    }

    fn test_cert() -> CertificateDer<'static> {
        CertificateDer::from(vec![0x30, 0x03, 0x02, 0x01, 0x00])
    }

    fn test_server_name() -> ServerName<'static> {
        ServerName::try_from("example.com").expect("valid test server name")
    }

    fn test_time() -> UnixTime {
        UnixTime::since_unix_epoch(Duration::from_secs(0))
    }

    fn test_signature() -> DigitallySignedStruct {
        use rustls::internal::msgs::codec::{Codec, Reader};

        let mut encoded = Vec::new();
        SignatureScheme::RSA_PKCS1_SHA256.encode(&mut encoded);
        encoded.extend([0, 3, 1, 2, 3]);
        DigitallySignedStruct::read(&mut Reader::init(&encoded))
            .expect("valid digitally signed struct")
    }

    #[test]
    fn cache_returns_same_arc_for_equal_config_keys() {
        ensure_crypto_provider();
        let config_a = get_or_build_config(&validation(false)).expect("config should build");
        let config_b = get_or_build_config(&validation(false)).expect("config should build");

        assert!(Arc::ptr_eq(&config_a, &config_b));
    }

    #[test]
    fn accept_all_verifier_accepts_any_cert_and_signature() {
        let verifier = AcceptAllCertVerifier;
        let cert = test_cert();
        let server_name = test_server_name();
        let dss = test_signature();

        assert!(
            verifier
                .verify_server_cert(&cert, &[], &server_name, &[], test_time())
                .is_ok()
        );
        assert!(verifier.verify_tls12_signature(&[], &cert, &dss).is_ok());
        assert!(verifier.verify_tls13_signature(&[], &cert, &dss).is_ok());
    }

    #[derive(Debug)]
    enum MockVerifyResult {
        Verified,
        NotValidForName,
        OtherError,
    }

    #[derive(Debug)]
    struct MockVerifier {
        result: MockVerifyResult,
    }

    impl ServerCertVerifier for MockVerifier {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, RustlsError> {
            match self.result {
                MockVerifyResult::Verified => Ok(ServerCertVerified::assertion()),
                MockVerifyResult::NotValidForName => Err(RustlsError::InvalidCertificate(
                    rustls::CertificateError::NotValidForName,
                )),
                MockVerifyResult::OtherError => Err(RustlsError::General("boom".to_string())),
            }
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, RustlsError> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, RustlsError> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            vec![SignatureScheme::RSA_PKCS1_SHA256]
        }
    }

    #[test]
    fn skip_hostname_verifier_treats_name_mismatch_as_success() {
        let verifier = SkipHostnameVerifier {
            inner: Arc::new(MockVerifier {
                result: MockVerifyResult::NotValidForName,
            }),
        };
        let cert = test_cert();
        let server_name = test_server_name();

        assert!(
            verifier
                .verify_server_cert(&cert, &[], &server_name, &[], test_time())
                .is_ok()
        );
    }

    #[test]
    fn skip_hostname_verifier_propagates_other_errors() {
        let verifier = SkipHostnameVerifier {
            inner: Arc::new(MockVerifier {
                result: MockVerifyResult::OtherError,
            }),
        };
        let cert = test_cert();
        let server_name = test_server_name();

        assert!(
            verifier
                .verify_server_cert(&cert, &[], &server_name, &[], test_time())
                .is_err()
        );
    }

    #[test]
    fn skip_hostname_verifier_propagates_success() {
        let verifier = SkipHostnameVerifier {
            inner: Arc::new(MockVerifier {
                result: MockVerifyResult::Verified,
            }),
        };
        let cert = test_cert();
        let server_name = test_server_name();

        assert!(
            verifier
                .verify_server_cert(&cert, &[], &server_name, &[], test_time())
                .is_ok()
        );
    }

    #[test]
    fn build_client_config_sets_alpn_only_when_requested() {
        ensure_crypto_provider();

        let with_alpn = build_client_config(&validation(true)).expect("config should build");
        assert_eq!(
            with_alpn.alpn_protocols,
            vec![TDS_8_ALPN_PROTOCOL.as_bytes().to_vec()]
        );

        let without_alpn = build_client_config(&validation(false)).expect("config should build");
        assert!(without_alpn.alpn_protocols.is_empty());
    }
}
