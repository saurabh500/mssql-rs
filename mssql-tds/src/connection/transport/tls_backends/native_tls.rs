// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! `native-tls` implementation of [`TlsBackend`].

use std::collections::HashMap;
use std::sync::RwLock;

use async_trait::async_trait;
use native_tls::TlsConnector as NativeTlsConnector;
use tokio_native_tls::TlsStream;
use tracing::info;

use crate::connection::transport::network_transport::Stream;
use crate::connection::transport::tls_backend::{
    TlsBackend, TlsHandshakeOutcome, TlsValidationConfig,
};
use crate::core::{TDS_8_ALPN_PROTOCOL, TdsResult};

/// Cache of pre-built `NativeTlsConnector` instances keyed by validation
/// config. Building a connector is expensive (~50ms on Linux) because
/// `native-tls` loads and parses the system CA certificate store via OpenSSL
/// on every call to `builder().build()`. Caching avoids this cost on
/// subsequent connections.
static CONNECTOR_CACHE: std::sync::LazyLock<
    RwLock<HashMap<TlsValidationConfig, NativeTlsConnector>>,
> = std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

fn get_or_build_connector(validation: &TlsValidationConfig) -> TdsResult<NativeTlsConnector> {
    if let Some(connector) = CONNECTOR_CACHE
        .read()
        .map_err(|_| {
            crate::error::Error::ImplementationError(
                "TLS connector cache read lock poisoned".to_string(),
            )
        })?
        .get(validation)
    {
        return Ok(connector.clone());
    }

    let mut builder = NativeTlsConnector::builder();
    if validation.accept_invalid_certs {
        builder.danger_accept_invalid_certs(true);
    }
    if validation.accept_invalid_hostnames {
        builder.danger_accept_invalid_hostnames(true);
    }
    if validation.use_alpn {
        builder.request_alpns(&[TDS_8_ALPN_PROTOCOL]);
    }
    let connector = builder.build()?;

    CONNECTOR_CACHE
        .write()
        .map_err(|_| {
            crate::error::Error::ImplementationError(
                "TLS connector cache write lock poisoned".to_string(),
            )
        })?
        .insert(validation.clone(), connector.clone());
    Ok(connector)
}

/// `native-tls` (OpenSSL / SChannel / Security.framework) backend.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct NativeTlsBackend;

#[async_trait]
impl TlsBackend for NativeTlsBackend {
    async fn perform_handshake(
        &self,
        validation: &TlsValidationConfig,
        host_name: &str,
        base_stream: Box<dyn Stream>,
    ) -> TdsResult<TlsHandshakeOutcome> {
        let connector = get_or_build_connector(validation)?;

        info!("Starting native-tls handshake using host {}", host_name);
        let tls_stream = tokio_native_tls::TlsConnector::from(connector)
            .connect(host_name, base_stream)
            .await
            .map_err(|e| crate::error::Error::TlsHandshakeError {
                source: e,
                expected_host: host_name.to_string(),
                // We can't retrieve cert SANs from a failed handshake — the
                // connection has already been torn down.
                cert_sans: "(unavailable - handshake failed before certificate could be retrieved)"
                    .to_string(),
            })?;

        let negotiated_alpn = if validation.use_alpn {
            tls_stream.get_ref().negotiated_alpn().ok().flatten()
        } else {
            None
        };

        // Best-effort peer cert extraction. SslHandler only inspects this when
        // ServerCertificate pinning is requested; a None here surfaces as
        // NoServerCertificate at the orchestration layer.
        let peer_cert_der = tls_stream
            .get_ref()
            .peer_certificate()
            .ok()
            .flatten()
            .and_then(|c| c.to_der().ok());

        Ok(TlsHandshakeOutcome {
            stream: Box::new(tls_stream),
            negotiated_alpn,
            peer_cert_der,
        })
    }
}

// Forwards the TDS-level handshake hooks down through the native-tls wrapper
// chain to the underlying boxed stream.
impl Stream for TlsStream<Box<dyn Stream>> {
    fn tls_handshake_starting(&mut self) {
        // tokio_native_tls::TlsStream -> native_tls::TlsStream -> AllowStd -> Box<dyn Stream>
        self.get_mut().get_mut().get_mut().tls_handshake_starting();
    }

    fn tls_handshake_completed(&mut self) {
        self.get_mut().get_mut().get_mut().tls_handshake_completed();
    }

    fn is_connection_dead(&self) -> bool {
        self.get_ref().get_ref().get_ref().is_connection_dead()
    }
}
