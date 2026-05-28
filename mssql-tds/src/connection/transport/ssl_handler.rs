// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Backend-agnostic TLS orchestration for TDS connections.
//!
//! [`SslHandler`] performs all of the work that is independent of the
//! underlying TLS library: validating the user's encryption options, deciding
//! the TLS validation profile, picking the host name to authenticate against,
//! and (when requested) pinning the server certificate after a successful
//! handshake. The actual TLS handshake is delegated to an implementation of
//! [`TlsBackend`] supplied by the caller, which defaults to whatever
//! [`tls_backends::SelectedTlsBackend`] resolves to at compile time.

use tracing::{info, warn};

use crate::connection::transport::certificate_validator;
use crate::connection::transport::network_transport::Stream;
use crate::connection::transport::tls_backend::{
    TlsBackend, TlsHandshakeOutcome, TlsValidationConfig,
};
use crate::connection::transport::tls_backends;
use crate::core::{EncryptionOptions, EncryptionSetting, NegotiatedEncryptionSetting, TdsResult};

#[derive(Debug)]
pub(crate) struct SslHandler {
    pub(crate) server_host_name: String,
    pub(crate) encryption_options: EncryptionOptions,
}

impl SslHandler {
    /// Determine TLS certificate validation behavior based on encryption options
    /// and the negotiated encryption mode.
    pub(crate) fn resolve_tls_validation(
        encryption_options: &EncryptionOptions,
        negotiated_encryption: NegotiatedEncryptionSetting,
    ) -> TlsValidationConfig {
        let use_alpn = negotiated_encryption == NegotiatedEncryptionSetting::Strict;

        if encryption_options.server_certificate.is_some() {
            // Certificate pinning mode: bypass CA validation, custom check later
            TlsValidationConfig {
                accept_invalid_certs: true,
                accept_invalid_hostnames: true,
                use_alpn,
            }
        } else if negotiated_encryption == NegotiatedEncryptionSetting::LoginOnly {
            // ODBC parity: LoginOnly skips cert validation unconditionally
            TlsValidationConfig {
                accept_invalid_certs: true,
                accept_invalid_hostnames: false,
                use_alpn,
            }
        } else if encryption_options.trust_server_certificate
            && encryption_options.mode != EncryptionSetting::Strict
        {
            TlsValidationConfig {
                accept_invalid_certs: true,
                accept_invalid_hostnames: false,
                use_alpn,
            }
        } else {
            TlsValidationConfig {
                accept_invalid_certs: false,
                accept_invalid_hostnames: false,
                use_alpn,
            }
        }
    }

    /// Enable TLS on `base_stream` using the compile-time default backend.
    pub(crate) async fn enable_ssl_async(
        &self,
        base_stream: Box<dyn Stream>,
        negotiated_encryption: NegotiatedEncryptionSetting,
    ) -> TdsResult<Box<dyn Stream>> {
        self.enable_ssl_async_with_backend(
            &tls_backends::selected_backend(),
            base_stream,
            negotiated_encryption,
        )
        .await
    }

    /// Enable TLS on `base_stream` using an explicit backend. Kept generic so
    /// alternate backends can be plugged in without touching the orchestration
    /// logic.
    pub(crate) async fn enable_ssl_async_with_backend<B: TlsBackend + ?Sized>(
        &self,
        backend: &B,
        mut base_stream: Box<dyn Stream>,
        negotiated_encryption: NegotiatedEncryptionSetting,
    ) -> TdsResult<Box<dyn Stream>> {
        base_stream.tls_handshake_starting();

        if self.encryption_options.server_certificate.is_some()
            && self.encryption_options.trust_server_certificate
        {
            warn!(
                "Both ServerCertificate and TrustServerCertificate are specified. ServerCertificate takes precedence."
            );
        }

        if self.encryption_options.server_certificate.is_some()
            && self.encryption_options.host_name_in_cert.is_some()
        {
            return Err(crate::error::Error::UsageError(
                "ServerCertificate and HostnameInCertificate are mutually exclusive. Use only one."
                    .to_string(),
            ));
        }

        if self.encryption_options.trust_server_certificate
            && self.encryption_options.mode == EncryptionSetting::Strict
        {
            warn!(
                "TrustServerCertificate is ignored for Strict encryption mode. Certificate validation will be enforced."
            );
        }

        let validation =
            Self::resolve_tls_validation(&self.encryption_options, negotiated_encryption);

        let host_name = self
            .encryption_options
            .host_name_in_cert
            .as_ref()
            .map_or_else(
                || &self.server_host_name,
                |host_name| {
                    if host_name.is_empty() {
                        &self.server_host_name
                    } else {
                        host_name
                    }
                },
            );

        info!(
            "TLS config: encryption_mode={:?}, trust_server_certificate={}, server_certificate={:?}, host_name_in_cert={:?}, resolved_host_name={}, server_host_name={}",
            self.encryption_options.mode,
            self.encryption_options.trust_server_certificate,
            self.encryption_options.server_certificate,
            self.encryption_options.host_name_in_cert,
            host_name,
            self.server_host_name,
        );

        let TlsHandshakeOutcome {
            mut stream,
            negotiated_alpn,
            peer_cert_der,
        } = backend
            .perform_handshake(&validation, host_name, base_stream)
            .await?;

        if validation.use_alpn {
            match negotiated_alpn {
                Some(ref proto) => {
                    tracing::debug!("Server negotiated ALPN: {}", String::from_utf8_lossy(proto));
                }
                None => {
                    tracing::debug!("Server did not negotiate an ALPN protocol");
                }
            }
        }

        if let Some(cert_path) = &self.encryption_options.server_certificate {
            info!("Validating server certificate using: {cert_path:?}");

            let server_cert_der = peer_cert_der.ok_or(crate::error::Error::NoServerCertificate)?;
            certificate_validator::validate_server_certificate(cert_path, &server_cert_der)?;

            info!("Server certificate validation successful");
        }

        stream.tls_handshake_completed();
        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_options() -> EncryptionOptions {
        EncryptionOptions {
            mode: EncryptionSetting::Required,
            trust_server_certificate: false,
            host_name_in_cert: None,
            server_certificate: None,
        }
    }

    #[test]
    fn login_only_skips_cert_validation() {
        let opts = default_options();
        let config =
            SslHandler::resolve_tls_validation(&opts, NegotiatedEncryptionSetting::LoginOnly);
        assert!(config.accept_invalid_certs);
        assert!(!config.accept_invalid_hostnames);
        assert!(!config.use_alpn);
    }

    #[test]
    fn login_only_skips_cert_validation_even_with_trust_false() {
        let mut opts = default_options();
        opts.trust_server_certificate = false;
        opts.mode = EncryptionSetting::PreferOff;
        let config =
            SslHandler::resolve_tls_validation(&opts, NegotiatedEncryptionSetting::LoginOnly);
        assert!(config.accept_invalid_certs);
        assert!(!config.use_alpn);
    }

    #[test]
    fn mandatory_without_trust_enforces_validation() {
        let opts = default_options();
        let config =
            SslHandler::resolve_tls_validation(&opts, NegotiatedEncryptionSetting::Mandatory);
        assert!(!config.accept_invalid_certs);
        assert!(!config.accept_invalid_hostnames);
        assert!(!config.use_alpn);
    }

    #[test]
    fn mandatory_with_trust_skips_cert_validation() {
        let mut opts = default_options();
        opts.trust_server_certificate = true;
        let config =
            SslHandler::resolve_tls_validation(&opts, NegotiatedEncryptionSetting::Mandatory);
        assert!(config.accept_invalid_certs);
        assert!(!config.accept_invalid_hostnames);
        assert!(!config.use_alpn);
    }

    #[test]
    fn strict_ignores_trust_server_certificate() {
        let mut opts = default_options();
        opts.mode = EncryptionSetting::Strict;
        opts.trust_server_certificate = true;
        let config = SslHandler::resolve_tls_validation(&opts, NegotiatedEncryptionSetting::Strict);
        assert!(!config.accept_invalid_certs);
        assert!(!config.accept_invalid_hostnames);
        assert!(config.use_alpn);
    }

    #[test]
    fn server_certificate_enables_pinning_mode() {
        let mut opts = default_options();
        opts.server_certificate = Some("cert.pem".into());
        let config =
            SslHandler::resolve_tls_validation(&opts, NegotiatedEncryptionSetting::Mandatory);
        assert!(config.accept_invalid_certs);
        assert!(config.accept_invalid_hostnames);
        assert!(!config.use_alpn);
    }

    #[test]
    fn server_certificate_takes_precedence_over_login_only() {
        let mut opts = default_options();
        opts.server_certificate = Some("cert.pem".into());
        let config =
            SslHandler::resolve_tls_validation(&opts, NegotiatedEncryptionSetting::LoginOnly);
        assert!(config.accept_invalid_certs);
        assert!(config.accept_invalid_hostnames);
        assert!(!config.use_alpn);
    }

    #[test]
    fn no_encryption_enforces_validation() {
        let opts = default_options();
        let config =
            SslHandler::resolve_tls_validation(&opts, NegotiatedEncryptionSetting::NoEncryption);
        assert!(!config.accept_invalid_certs);
        assert!(!config.accept_invalid_hostnames);
        assert!(!config.use_alpn);
    }

    #[test]
    fn strict_enables_alpn() {
        let opts = default_options();
        let config = SslHandler::resolve_tls_validation(&opts, NegotiatedEncryptionSetting::Strict);
        assert!(config.use_alpn);
    }

    #[test]
    fn non_strict_modes_disable_alpn() {
        let opts = default_options();
        for mode in [
            NegotiatedEncryptionSetting::Mandatory,
            NegotiatedEncryptionSetting::LoginOnly,
            NegotiatedEncryptionSetting::NoEncryption,
        ] {
            let config = SslHandler::resolve_tls_validation(&opts, mode);
            assert!(!config.use_alpn, "use_alpn should be false for {:?}", mode);
        }
    }

    #[test]
    fn strict_with_server_certificate_enables_alpn() {
        let mut opts = default_options();
        opts.server_certificate = Some("cert.pem".into());
        let config = SslHandler::resolve_tls_validation(&opts, NegotiatedEncryptionSetting::Strict);
        assert!(config.use_alpn);
        assert!(config.accept_invalid_certs);
        assert!(config.accept_invalid_hostnames);
    }
}
