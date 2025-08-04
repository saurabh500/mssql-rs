// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use crate::error::Error;
use crate::error::Error::OperationCancelledError;
use std::future::Future;
use tokio_util::sync::CancellationToken;

pub type TdsResult<T> = Result<T, Error>;

pub struct CancelHandle {
    pub(crate) cancel_token: CancellationToken,
}

impl CancelHandle {
    pub fn new() -> Self {
        CancelHandle {
            cancel_token: CancellationToken::new(),
        }
    }

    pub fn cancel(self) {
        self.cancel_token.cancel();
    }

    pub fn child_handle(&self) -> Self {
        Self::from(self.cancel_token.child_token())
    }

    pub(crate) async fn run_until_cancelled<F, ResultType>(
        cancel_handle: Option<&CancelHandle>,
        f: F,
    ) -> F::Output
    where
        F: Future<Output = TdsResult<ResultType>> + Send,
    {
        match cancel_handle {
            Some(handle) => match handle.cancel_token.run_until_cancelled(f).await {
                Some(result) => result,
                None => Err(OperationCancelledError("Request was cancelled".to_string())),
            },
            None => f.await,
        }
    }
}

impl From<CancellationToken> for CancelHandle {
    fn from(value: CancellationToken) -> Self {
        CancelHandle {
            cancel_token: value,
        }
    }
}

impl Default for CancelHandle {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(PartialEq, Debug)]
pub enum SQLServerVersion {
    SqlServerNotsupported = 0,
    SqlServer2000 = 8,
    SqlServer2005 = 9,
    SqlServer2008 = 10, // This also represents 2008R2.
    SqlServer2012 = 11,
    SqlServer2014 = 12,
    SqlServer2016 = 13,
    SqlServer2017 = 14,
    SqlServer2019 = 15,
    SqlServer2022 = 16,
    SqlServer2022lus = 17,
}

impl From<u8> for SQLServerVersion {
    fn from(v: u8) -> Self {
        match v {
            0 => SQLServerVersion::SqlServerNotsupported,
            8 => SQLServerVersion::SqlServer2000,
            9 => SQLServerVersion::SqlServer2005,
            10 => SQLServerVersion::SqlServer2008,
            11 => SQLServerVersion::SqlServer2012,
            12 => SQLServerVersion::SqlServer2014,
            13 => SQLServerVersion::SqlServer2016,
            14 => SQLServerVersion::SqlServer2017,
            15 => SQLServerVersion::SqlServer2019,
            16 => SQLServerVersion::SqlServer2022,
            17 => SQLServerVersion::SqlServer2022lus,
            _ => SQLServerVersion::SqlServerNotsupported,
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct Version {
    pub major: u8,
    pub minor: u8,
    pub build: u16,
    pub revision: u16,
}

impl Version {
    pub fn new(major: u8, minor: u8, build: u16, revision: u16) -> Self {
        Version {
            major,
            minor,
            build,
            revision,
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct EncryptionOptions {
    pub mode: EncryptionSetting,
    pub trust_server_certificate: bool,
    pub host_name_in_cert: Option<String>,
}

impl EncryptionOptions {
    pub fn new() -> Self {
        EncryptionOptions {
            mode: EncryptionSetting::Strict,
            trust_server_certificate: false,
            host_name_in_cert: None,
        }
    }
}

impl Default for EncryptionOptions {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum EncryptionSetting {
    PreferOff, // Don't use encryption if the server allows this or doesn't support encryption.
    On,        // Use encryption after prelogin.
    Required, // Use encryption after prelogin. Note that this is the same as On per the documentation,
    // but writes a different value in the request. Perhaps On is supposed to keep the connection alive,
    // but unencrypted if the server returns encryption off/unsupported.
    Strict, // Encrypt the whole stream, including prelogin.
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum NegotiatedEncryptionSetting {
    Strict,
    LoginOnly,
    Mandatory,
    NoEncryption,
}
