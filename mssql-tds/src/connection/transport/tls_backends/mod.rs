// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Concrete TLS backend implementations.
//!
//! The active backend is exposed as [`SelectedTlsBackend`] and is selected at
//! compile time. With only the `native-tls` backend present today this is a
//! plain alias; once additional backends (e.g. `rustls`) are added each will
//! be cfg-gated and the alias will resolve to whichever was chosen.

pub(crate) mod native_tls;

pub(crate) use self::native_tls::NativeTlsBackend;

/// The TLS backend used for all handshakes in this build.
pub(crate) type SelectedTlsBackend = NativeTlsBackend;

/// Returns a fresh instance of the selected backend. Backends are unit structs
/// so this is essentially free.
pub(crate) fn selected_backend() -> SelectedTlsBackend {
    NativeTlsBackend
}
