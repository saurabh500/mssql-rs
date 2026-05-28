// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(not(any(feature = "native-tls-backend", feature = "rustls-backend")))]
compile_error!(
    "mssql-tds requires exactly one TLS backend feature: enable `native-tls-backend` or `rustls-backend`."
);

// The workspace clippy alias uses --all-features; real builds still reject both.
#[cfg(all(
    feature = "native-tls-backend",
    feature = "rustls-backend",
    not(clippy)
))]
compile_error!(
    "mssql-tds TLS backend features are mutually exclusive: enable only one of `native-tls-backend` or `rustls-backend`."
);

#[cfg(feature = "native-tls-backend")]
pub(crate) mod native_tls;
#[cfg(all(
    feature = "rustls-backend",
    any(not(feature = "native-tls-backend"), not(clippy))
))]
pub(crate) mod rustls;

#[cfg(all(
    feature = "native-tls-backend",
    any(not(feature = "rustls-backend"), clippy)
))]
pub(crate) type SelectedTlsBackend = self::native_tls::NativeTlsBackend;
#[cfg(all(feature = "rustls-backend", not(feature = "native-tls-backend")))]
pub(crate) type SelectedTlsBackend = self::rustls::RustlsBackend;

#[cfg(all(
    feature = "native-tls-backend",
    any(not(feature = "rustls-backend"), clippy)
))]
pub(crate) fn selected_backend() -> SelectedTlsBackend {
    self::native_tls::NativeTlsBackend
}
#[cfg(all(feature = "rustls-backend", not(feature = "native-tls-backend")))]
pub(crate) fn selected_backend() -> SelectedTlsBackend {
    self::rustls::RustlsBackend
}
