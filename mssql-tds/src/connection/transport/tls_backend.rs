// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! TLS backend abstraction.
//!
//! `SslHandler` performs all the backend-agnostic work for enabling TLS on a
//! TDS connection: input validation, host-name fallback, logging, and post-
//! handshake certificate pinning. The concrete TLS handshake itself is
//! delegated to an implementation of the [`TlsBackend`] trait.
//!
//! Today the only implementation is [`tls_backends::native_tls::NativeTlsBackend`].
//! The trait is shaped so that an alternative backend (e.g. rustls) can be
//! added without touching `SslHandler` or any call site.

use async_trait::async_trait;

use crate::connection::transport::network_transport::Stream;
use crate::core::TdsResult;

/// The set of validation knobs a backend needs in order to build a TLS
/// connector. Computed by [`crate::connection::transport::ssl_handler::SslHandler::resolve_tls_validation`]
/// from the user's [`crate::core::EncryptionOptions`] and the negotiated encryption mode.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TlsValidationConfig {
    pub accept_invalid_certs: bool,
    pub accept_invalid_hostnames: bool,
    pub use_alpn: bool,
}

/// What a backend hands back after a successful handshake.
///
/// `peer_cert_der` is populated whenever the backend can cheaply expose the
/// server's leaf certificate. `SslHandler` only inspects it when the user
/// supplied a `ServerCertificate` for pinning; otherwise it is ignored.
pub(crate) struct TlsHandshakeOutcome {
    pub stream: Box<dyn Stream>,
    pub negotiated_alpn: Option<Vec<u8>>,
    pub peer_cert_der: Option<Vec<u8>>,
}

/// Pluggable TLS handshake driver.
///
/// Implementations own their own connector cache and are expected to be cheap
/// to clone or instantiate (a unit struct is fine — the cache lives in a
/// `LazyLock` inside the backend module).
#[async_trait]
pub(crate) trait TlsBackend: Send + Sync + std::fmt::Debug {
    /// Run the TLS client handshake over `base_stream`, returning the wrapped
    /// stream plus any negotiated ALPN protocol and the peer leaf cert.
    ///
    /// On handshake failure the backend is responsible for returning a
    /// [`crate::error::Error::TlsHandshakeError`] populated with `host_name`.
    async fn perform_handshake(
        &self,
        validation: &TlsValidationConfig,
        host_name: &str,
        base_stream: Box<dyn Stream>,
    ) -> TdsResult<TlsHandshakeOutcome>;
}
