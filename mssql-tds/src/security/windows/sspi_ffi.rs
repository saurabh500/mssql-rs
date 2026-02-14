// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! FFI bindings for Windows SSPI (Security Support Provider Interface).
//!
//! This module provides low-level bindings to the Windows Security API
//! functions from `secur32.dll`.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]

use std::ffi::c_void;
use std::ptr;

// =============================================================================
// Type Definitions
// =============================================================================

/// SECURITY_STATUS return type (HRESULT-like)
pub type SECURITY_STATUS = i32;

/// Wide character type for Windows Unicode APIs
pub type WCHAR = u16;
pub type LPCWSTR = *const WCHAR;
pub type LPWSTR = *mut WCHAR;

/// Unsigned long types
pub type ULONG = u32;
pub type PULONG = *mut ULONG;

/// Pointer to void
pub type PVOID = *mut c_void;

/// TimeStamp structure (FILETIME equivalent)
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct TimeStamp {
    pub LowPart: u32,
    pub HighPart: i32,
}

/// Credential handle
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct CredHandle {
    pub dwLower: usize,
    pub dwUpper: usize,
}

impl CredHandle {
    pub fn is_valid(&self) -> bool {
        self.dwLower != 0 || self.dwUpper != 0
    }
}

/// Security context handle (same structure as CredHandle)
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct CtxtHandle {
    pub dwLower: usize,
    pub dwUpper: usize,
}

impl CtxtHandle {
    pub fn is_valid(&self) -> bool {
        self.dwLower != 0 || self.dwUpper != 0
    }
}

/// Security buffer for input/output data
#[repr(C)]
#[derive(Debug)]
pub struct SecBuffer {
    /// Size of the buffer in bytes
    pub cbBuffer: ULONG,
    /// Type of buffer (SECBUFFER_TOKEN, etc.)
    pub BufferType: ULONG,
    /// Pointer to buffer data
    pub pvBuffer: PVOID,
}

impl Default for SecBuffer {
    fn default() -> Self {
        Self {
            cbBuffer: 0,
            BufferType: SECBUFFER_EMPTY,
            pvBuffer: ptr::null_mut(),
        }
    }
}

/// Security buffer descriptor
#[repr(C)]
#[derive(Debug)]
pub struct SecBufferDesc {
    /// Version number (always SECBUFFER_VERSION)
    pub ulVersion: ULONG,
    /// Number of buffers
    pub cBuffers: ULONG,
    /// Pointer to array of SecBuffer structures
    pub pBuffers: *mut SecBuffer,
}

impl Default for SecBufferDesc {
    fn default() -> Self {
        Self {
            ulVersion: SECBUFFER_VERSION,
            cBuffers: 0,
            pBuffers: ptr::null_mut(),
        }
    }
}

/// SEC_WINNT_AUTH_IDENTITY_W structure for explicit credentials
#[repr(C)]
pub struct SEC_WINNT_AUTH_IDENTITY_W {
    pub User: LPWSTR,
    pub UserLength: ULONG,
    pub Domain: LPWSTR,
    pub DomainLength: ULONG,
    pub Password: LPWSTR,
    pub PasswordLength: ULONG,
    pub Flags: ULONG,
}

/// Security package information
#[repr(C)]
pub struct SecPkgInfoW {
    pub fCapabilities: ULONG,
    pub wVersion: u16,
    pub wRPCID: u16,
    pub cbMaxToken: ULONG,
    pub Name: LPWSTR,
    pub Comment: LPWSTR,
}

// =============================================================================
// Constants - SECURITY_STATUS values
// =============================================================================

/// Success
pub const SEC_E_OK: SECURITY_STATUS = 0;

/// Continue needed - not an error, more rounds required
pub const SEC_I_CONTINUE_NEEDED: SECURITY_STATUS = 0x00090312_u32 as i32;

/// Complete needed - call CompleteAuthToken
pub const SEC_I_COMPLETE_NEEDED: SECURITY_STATUS = 0x00090313_u32 as i32;

/// Complete and continue - call CompleteAuthToken and continue
pub const SEC_I_COMPLETE_AND_CONTINUE: SECURITY_STATUS = 0x00090314_u32 as i32;

/// Logon was denied
pub const SEC_E_LOGON_DENIED: SECURITY_STATUS = 0x8009030C_u32 as i32;

/// Target unknown (SPN not found)
pub const SEC_E_TARGET_UNKNOWN: SECURITY_STATUS = 0x80090303_u32 as i32;

/// Invalid handle
pub const SEC_E_INVALID_HANDLE: SECURITY_STATUS = 0x80090301_u32 as i32;

/// Invalid token
pub const SEC_E_INVALID_TOKEN: SECURITY_STATUS = 0x80090308_u32 as i32;

/// No credentials available
pub const SEC_E_NO_CREDENTIALS: SECURITY_STATUS = 0x8009030E_u32 as i32;

/// Credentials expired
pub const SEC_E_CONTEXT_EXPIRED: SECURITY_STATUS = 0x80090317_u32 as i32;

/// Internal error
pub const SEC_E_INTERNAL_ERROR: SECURITY_STATUS = 0x80090304_u32 as i32;

/// Insufficient memory
pub const SEC_E_INSUFFICIENT_MEMORY: SECURITY_STATUS = 0x80090300_u32 as i32;

/// Buffer too small
pub const SEC_E_BUFFER_TOO_SMALL: SECURITY_STATUS = 0x80090321_u32 as i32;

/// Wrong principal
pub const SEC_E_WRONG_PRINCIPAL: SECURITY_STATUS = 0x80090322_u32 as i32;

/// Unsupported function
pub const SEC_E_UNSUPPORTED_FUNCTION: SECURITY_STATUS = 0x80090302_u32 as i32;

// =============================================================================
// Constants - Credential use
// =============================================================================

/// Outbound credentials (client-side)
pub const SECPKG_CRED_OUTBOUND: ULONG = 2;

/// Inbound credentials (server-side)
pub const SECPKG_CRED_INBOUND: ULONG = 1;

/// Both directions
pub const SECPKG_CRED_BOTH: ULONG = 3;

// =============================================================================
// Constants - InitializeSecurityContext flags
// =============================================================================

/// Allow credential delegation
pub const ISC_REQ_DELEGATE: ULONG = 0x00000001;

/// Mutual authentication required
pub const ISC_REQ_MUTUAL_AUTH: ULONG = 0x00000002;

/// Replay detection
pub const ISC_REQ_REPLAY_DETECT: ULONG = 0x00000004;

/// Sequence detection
pub const ISC_REQ_SEQUENCE_DETECT: ULONG = 0x00000008;

/// Confidentiality (encryption)
pub const ISC_REQ_CONFIDENTIALITY: ULONG = 0x00000010;

/// Use supplied credentials
pub const ISC_REQ_USE_SUPPLIED_CREDS: ULONG = 0x00000080;

/// Allocate output buffer
pub const ISC_REQ_ALLOCATE_MEMORY: ULONG = 0x00000100;

/// Use datagram-style communication
pub const ISC_REQ_DATAGRAM: ULONG = 0x00000400;

/// Request connection-oriented communication
pub const ISC_REQ_CONNECTION: ULONG = 0x00000800;

/// Extended error information
pub const ISC_REQ_EXTENDED_ERROR: ULONG = 0x00004000;

/// Stream-oriented communication
pub const ISC_REQ_STREAM: ULONG = 0x00008000;

/// Message integrity
pub const ISC_REQ_INTEGRITY: ULONG = 0x00010000;

/// Standard flags for SQL Server authentication (from ODBC driver)
pub const STANDARD_CONTEXT_REQ: ULONG =
    ISC_REQ_DELEGATE | ISC_REQ_MUTUAL_AUTH | ISC_REQ_INTEGRITY | ISC_REQ_EXTENDED_ERROR;

// =============================================================================
// Constants - Security buffer types
// =============================================================================

/// Empty buffer placeholder
pub const SECBUFFER_EMPTY: ULONG = 0;

/// Security token data
pub const SECBUFFER_TOKEN: ULONG = 2;

/// Package-specific parameters
pub const SECBUFFER_PKG_PARAMS: ULONG = 3;

/// Missing data indicator
pub const SECBUFFER_MISSING: ULONG = 4;

/// Extra data beyond message
pub const SECBUFFER_EXTRA: ULONG = 5;

/// Stream trailer
pub const SECBUFFER_STREAM_TRAILER: ULONG = 6;

/// Stream header
pub const SECBUFFER_STREAM_HEADER: ULONG = 7;

/// Channel bindings
pub const SECBUFFER_CHANNEL_BINDINGS: ULONG = 14;

/// Target host name
pub const SECBUFFER_TARGET_HOST: ULONG = 16;

/// Buffer version
pub const SECBUFFER_VERSION: ULONG = 0;

// =============================================================================
// Constants - Auth identity flags
// =============================================================================

/// ANSI strings in identity structure
pub const SEC_WINNT_AUTH_IDENTITY_ANSI: ULONG = 1;

/// Unicode strings in identity structure
pub const SEC_WINNT_AUTH_IDENTITY_UNICODE: ULONG = 2;

// =============================================================================
// FFI Function Declarations
// =============================================================================

#[link(name = "secur32")]
unsafe extern "system" {
    /// Acquires a handle to preexisting credentials of a security principal.
    ///
    /// <https://docs.microsoft.com/en-us/windows/win32/api/sspi/nf-sspi-acquirecredentialshandlew>
    pub fn AcquireCredentialsHandleW(
        pszPrincipal: LPCWSTR,
        pszPackage: LPCWSTR,
        fCredentialUse: ULONG,
        pvLogonId: PVOID,
        pAuthData: PVOID,
        pGetKeyFn: PVOID,
        pvGetKeyArgument: PVOID,
        phCredential: *mut CredHandle,
        ptsExpiry: *mut TimeStamp,
    ) -> SECURITY_STATUS;

    /// Initiates the client side of an authentication sequence.
    ///
    /// <https://docs.microsoft.com/en-us/windows/win32/api/sspi/nf-sspi-initializesecuritycontextw>
    pub fn InitializeSecurityContextW(
        phCredential: *const CredHandle,
        phContext: *const CtxtHandle,
        pszTargetName: LPCWSTR,
        fContextReq: ULONG,
        Reserved1: ULONG,
        TargetDataRep: ULONG,
        pInput: *const SecBufferDesc,
        Reserved2: ULONG,
        phNewContext: *mut CtxtHandle,
        pOutput: *mut SecBufferDesc,
        pfContextAttr: PULONG,
        ptsExpiry: *mut TimeStamp,
    ) -> SECURITY_STATUS;

    /// Deletes the local data structures associated with the security context.
    ///
    /// <https://docs.microsoft.com/en-us/windows/win32/api/sspi/nf-sspi-deletesecuritycontext>
    pub fn DeleteSecurityContext(phContext: *mut CtxtHandle) -> SECURITY_STATUS;

    /// Frees the credential handle.
    ///
    /// <https://docs.microsoft.com/en-us/windows/win32/api/sspi/nf-sspi-freecredentialshandle>
    pub fn FreeCredentialsHandle(phCredential: *mut CredHandle) -> SECURITY_STATUS;

    /// Frees a memory buffer allocated by a security package.
    ///
    /// <https://docs.microsoft.com/en-us/windows/win32/api/sspi/nf-sspi-freecontextbuffer>
    pub fn FreeContextBuffer(pvContextBuffer: PVOID) -> SECURITY_STATUS;

    /// Queries information about a security package.
    ///
    /// <https://docs.microsoft.com/en-us/windows/win32/api/sspi/nf-sspi-querysecuritypackageinfow>
    pub fn QuerySecurityPackageInfoW(
        pszPackageName: LPCWSTR,
        ppPackageInfo: *mut *mut SecPkgInfoW,
    ) -> SECURITY_STATUS;

    /// Completes an authentication token (needed for some protocols like NTLM).
    ///
    /// <https://docs.microsoft.com/en-us/windows/win32/api/sspi/nf-sspi-completeauthtoken>
    pub fn CompleteAuthToken(
        phContext: *const CtxtHandle,
        pToken: *const SecBufferDesc,
    ) -> SECURITY_STATUS;
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Converts a SECURITY_STATUS to a human-readable error message.
pub fn get_sspi_error_message(status: SECURITY_STATUS) -> String {
    match status {
        SEC_E_OK => "Success".to_string(),
        SEC_I_CONTINUE_NEEDED => "Continue needed".to_string(),
        SEC_I_COMPLETE_NEEDED => "Complete needed".to_string(),
        SEC_I_COMPLETE_AND_CONTINUE => "Complete and continue".to_string(),
        SEC_E_LOGON_DENIED => "Logon denied".to_string(),
        SEC_E_TARGET_UNKNOWN => "Target unknown (SPN not found)".to_string(),
        SEC_E_INVALID_HANDLE => "Invalid handle".to_string(),
        SEC_E_INVALID_TOKEN => "Invalid token".to_string(),
        SEC_E_NO_CREDENTIALS => "No credentials available".to_string(),
        SEC_E_CONTEXT_EXPIRED => "Context expired".to_string(),
        SEC_E_INTERNAL_ERROR => "Internal error".to_string(),
        SEC_E_INSUFFICIENT_MEMORY => "Insufficient memory".to_string(),
        SEC_E_BUFFER_TOO_SMALL => "Buffer too small".to_string(),
        SEC_E_WRONG_PRINCIPAL => "Wrong principal".to_string(),
        SEC_E_UNSUPPORTED_FUNCTION => "Unsupported function".to_string(),
        _ => format!("SSPI error 0x{:08X}", status as u32),
    }
}

/// Converts a Rust string to a null-terminated wide string.
pub fn to_wide_string(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(std::iter::once(0)).collect()
}

/// Checks if a security status indicates success (including continue-needed).
pub fn is_success_status(status: SECURITY_STATUS) -> bool {
    status >= 0
}

/// Checks if a security status indicates that more rounds are needed.
pub fn needs_continue(status: SECURITY_STATUS) -> bool {
    status == SEC_I_CONTINUE_NEEDED || status == SEC_I_COMPLETE_AND_CONTINUE
}

/// Checks if CompleteAuthToken needs to be called.
pub fn needs_complete(status: SECURITY_STATUS) -> bool {
    status == SEC_I_COMPLETE_NEEDED || status == SEC_I_COMPLETE_AND_CONTINUE
}

/// Gets the maximum token size for a security package.
pub fn get_max_token_size(package_name: &str) -> Result<u32, SECURITY_STATUS> {
    let package_wide = to_wide_string(package_name);
    let mut pkg_info: *mut SecPkgInfoW = ptr::null_mut();

    let status = unsafe { QuerySecurityPackageInfoW(package_wide.as_ptr(), &mut pkg_info) };

    if status != SEC_E_OK {
        return Err(status);
    }

    let max_token = unsafe { (*pkg_info).cbMaxToken };

    // Free the package info
    unsafe {
        FreeContextBuffer(pkg_info as PVOID);
    }

    Ok(max_token)
}

/// Acquires credentials for the current user.
///
/// # Arguments
/// * `package_name` - Security package name ("Negotiate", "Kerberos", or "NTLM")
///
/// # Returns
/// * `Ok((CredHandle, TimeStamp))` - The credential handle and expiry time
/// * `Err(SECURITY_STATUS)` - The error status if acquisition failed
pub fn acquire_credentials(package_name: &str) -> Result<(CredHandle, TimeStamp), SECURITY_STATUS> {
    let package_wide = to_wide_string(package_name);
    let mut cred_handle = CredHandle::default();
    let mut expiry = TimeStamp::default();

    let status = unsafe {
        AcquireCredentialsHandleW(
            ptr::null(), // Use current user principal
            package_wide.as_ptr(),
            SECPKG_CRED_OUTBOUND,
            ptr::null_mut(), // No logon ID
            ptr::null_mut(), // No auth data (use current credentials)
            ptr::null_mut(), // No GetKey function
            ptr::null_mut(), // No GetKey argument
            &mut cred_handle,
            &mut expiry,
        )
    };

    if status != SEC_E_OK {
        return Err(status);
    }

    Ok((cred_handle, expiry))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_wide_string() {
        let wide = to_wide_string("Negotiate");
        assert_eq!(
            wide,
            vec![
                'N' as u16, 'e' as u16, 'g' as u16, 'o' as u16, 't' as u16, 'i' as u16, 'a' as u16,
                't' as u16, 'e' as u16, 0
            ]
        );
    }

    #[test]
    fn test_is_success_status() {
        assert!(is_success_status(SEC_E_OK));
        assert!(is_success_status(SEC_I_CONTINUE_NEEDED));
        assert!(!is_success_status(SEC_E_LOGON_DENIED));
    }

    #[test]
    fn test_needs_continue() {
        assert!(needs_continue(SEC_I_CONTINUE_NEEDED));
        assert!(needs_continue(SEC_I_COMPLETE_AND_CONTINUE));
        assert!(!needs_continue(SEC_E_OK));
    }

    #[test]
    fn test_cred_handle_default() {
        let handle = CredHandle::default();
        assert!(!handle.is_valid());
    }

    #[test]
    fn test_get_sspi_error_message() {
        assert_eq!(get_sspi_error_message(SEC_E_OK), "Success");
        assert_eq!(
            get_sspi_error_message(SEC_E_TARGET_UNKNOWN),
            "Target unknown (SPN not found)"
        );
    }
}
