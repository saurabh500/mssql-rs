// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! GSSAPI FFI bindings for Kerberos authentication.
//!
//! This module provides raw FFI bindings to the MIT Kerberos GSSAPI library
//! (`libgssapi_krb5`). These bindings are used by `GssapiContext` to perform
//! Kerberos authentication on Linux and macOS.
//!
//! # Safety
//!
//! All FFI functions are inherently unsafe. The safe wrappers in `GssapiContext`
//! should be used instead of calling these directly.

use std::os::raw::c_void;
use std::ptr;

/// GSSAPI OM_uint32 type (status codes and flags)
pub type GssOmUint32 = u32;

/// GSSAPI context handle (opaque pointer)
pub type GssCtxIdT = *mut c_void;

/// GSSAPI credential handle (opaque pointer)
pub type GssCredIdT = *mut c_void;

/// GSSAPI name handle (opaque pointer)
pub type GssNameT = *mut c_void;

/// GSSAPI OID structure
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct GssOidDesc {
    pub length: GssOmUint32,
    pub elements: *mut c_void,
}

/// GSSAPI OID pointer type
pub type GssOid = *mut GssOidDesc;

/// GSSAPI buffer descriptor
#[repr(C)]
#[derive(Debug)]
pub struct GssBufferDesc {
    pub length: usize,
    pub value: *mut c_void,
}

impl Default for GssBufferDesc {
    fn default() -> Self {
        Self {
            length: 0,
            value: ptr::null_mut(),
        }
    }
}

impl GssBufferDesc {
    /// Creates a buffer from a byte slice (for input)
    pub fn from_slice(data: &[u8]) -> Self {
        Self {
            length: data.len(),
            value: data.as_ptr() as *mut c_void,
        }
    }

    /// Creates a buffer from a string (for SPN)
    pub fn from_str(s: &str) -> Self {
        Self {
            length: s.len(),
            value: s.as_ptr() as *mut c_void,
        }
    }

    /// Converts the buffer to a byte vector (for output)
    ///
    /// # Safety
    ///
    /// The buffer must contain valid data of the specified length.
    pub unsafe fn to_vec(&self) -> Vec<u8> {
        if self.value.is_null() || self.length == 0 {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(self.value as *const u8, self.length).to_vec() }
        }
    }
}

/// GSSAPI buffer pointer type
pub type GssBufferT = *mut GssBufferDesc;

/// GSSAPI channel bindings structure
#[repr(C)]
#[derive(Debug)]
pub struct GssChannelBindingsStruct {
    pub initiator_addrtype: GssOmUint32,
    pub initiator_address: GssBufferDesc,
    pub acceptor_addrtype: GssOmUint32,
    pub acceptor_address: GssBufferDesc,
    pub application_data: GssBufferDesc,
}

pub type GssChannelBindingsT = *mut GssChannelBindingsStruct;

// =============================================================================
// GSSAPI Constants
// =============================================================================

/// Null context handle
pub const GSS_C_NO_CONTEXT: GssCtxIdT = ptr::null_mut();

/// Null credential handle (use default credentials)
pub const GSS_C_NO_CREDENTIAL: GssCredIdT = ptr::null_mut();

/// Null OID (use default mechanism)
pub const GSS_C_NO_OID: GssOid = ptr::null_mut();

/// Null channel bindings
pub const GSS_C_NO_CHANNEL_BINDINGS: GssChannelBindingsT = ptr::null_mut();

/// Null buffer
pub const GSS_C_NO_BUFFER: GssBufferT = ptr::null_mut();

// GSSAPI Major Status Codes (calling errors in high 16 bits, routine errors in bits 16-23)
/// Operation completed successfully
pub const GSS_S_COMPLETE: GssOmUint32 = 0;

/// Continuation call needed (not an error)
pub const GSS_S_CONTINUE_NEEDED: GssOmUint32 = 1 << 0; // 0x00000001

/// A required input parameter was bad
pub const GSS_S_BAD_MECH: GssOmUint32 = 1 << 16; // Unsupported mechanism

/// Invalid credentials
pub const GSS_S_NO_CRED: GssOmUint32 = 7 << 16; // No credentials available

/// Credentials expired
pub const GSS_S_CREDENTIALS_EXPIRED: GssOmUint32 = 11 << 16;

/// Invalid context
pub const GSS_S_NO_CONTEXT: GssOmUint32 = 8 << 16;

/// Defective token
pub const GSS_S_DEFECTIVE_TOKEN: GssOmUint32 = 9 << 16;

/// Invalid name
pub const GSS_S_BAD_NAME: GssOmUint32 = 2 << 16;

/// Name type not supported
pub const GSS_S_BAD_NAMETYPE: GssOmUint32 = 3 << 16;

/// General failure
pub const GSS_S_FAILURE: GssOmUint32 = 13 << 16;

// GSSAPI Request Flags
/// Request mutual authentication
pub const GSS_C_MUTUAL_FLAG: GssOmUint32 = 2;

/// Request credential delegation
pub const GSS_C_DELEG_FLAG: GssOmUint32 = 1;

/// Request replay detection
pub const GSS_C_REPLAY_FLAG: GssOmUint32 = 4;

/// Request sequence detection
pub const GSS_C_SEQUENCE_FLAG: GssOmUint32 = 8;

// GSSAPI Name Types (defined as static OIDs in the library)
// We'll get these from the library at runtime

// =============================================================================
// GSSAPI Function Declarations
// =============================================================================

// Force dynamic linking to support musl builds (Alpine Linux)
// musl's static linking requires .a files, but krb5-dev only provides .so files
#[link(name = "gssapi_krb5", kind = "dylib")]
unsafe extern "C" {
    /// Initialize a security context with a server.
    ///
    /// This is the main function for initiating authentication.
    pub fn gss_init_sec_context(
        minor_status: *mut GssOmUint32,
        cred_handle: GssCredIdT,
        context_handle: *mut GssCtxIdT,
        target_name: GssNameT,
        mech_type: GssOid,
        req_flags: GssOmUint32,
        time_req: GssOmUint32,
        input_chan_bindings: GssChannelBindingsT,
        input_token: GssBufferT,
        actual_mech_type: *mut GssOid,
        output_token: GssBufferT,
        ret_flags: *mut GssOmUint32,
        time_rec: *mut GssOmUint32,
    ) -> GssOmUint32;

    /// Import a name (like an SPN) into internal GSSAPI format.
    pub fn gss_import_name(
        minor_status: *mut GssOmUint32,
        input_name_buffer: GssBufferT,
        input_name_type: GssOid,
        output_name: *mut GssNameT,
    ) -> GssOmUint32;

    /// Release a GSSAPI buffer.
    pub fn gss_release_buffer(minor_status: *mut GssOmUint32, buffer: GssBufferT) -> GssOmUint32;

    /// Release a GSSAPI name.
    pub fn gss_release_name(minor_status: *mut GssOmUint32, name: *mut GssNameT) -> GssOmUint32;

    /// Delete a security context.
    pub fn gss_delete_sec_context(
        minor_status: *mut GssOmUint32,
        context_handle: *mut GssCtxIdT,
        output_token: GssBufferT,
    ) -> GssOmUint32;

    /// Display a status message for debugging.
    pub fn gss_display_status(
        minor_status: *mut GssOmUint32,
        status_value: GssOmUint32,
        status_type: i32,
        mech_type: GssOid,
        message_context: *mut GssOmUint32,
        status_string: GssBufferT,
    ) -> GssOmUint32;

    /// Acquire credentials (optional, for explicit credential management).
    pub fn gss_acquire_cred(
        minor_status: *mut GssOmUint32,
        desired_name: GssNameT,
        time_req: GssOmUint32,
        desired_mechs: *mut GssOidDesc, // gss_OID_set
        cred_usage: i32,
        output_cred_handle: *mut GssCredIdT,
        actual_mechs: *mut *mut GssOidDesc,
        time_rec: *mut GssOmUint32,
    ) -> GssOmUint32;

    /// Release credentials.
    pub fn gss_release_cred(
        minor_status: *mut GssOmUint32,
        cred_handle: *mut GssCredIdT,
    ) -> GssOmUint32;
}

// =============================================================================
// External OID constants (defined in libgssapi_krb5)
// =============================================================================

// Force dynamic linking to support musl builds (Alpine Linux)
#[link(name = "gssapi_krb5", kind = "dylib")]
unsafe extern "C" {
    /// GSS_C_NT_USER_NAME - User name string
    pub static gss_nt_user_name: GssOid;

    /// GSS_C_NT_HOSTBASED_SERVICE - Service name "service@host"
    pub static gss_nt_service_name: GssOid;

    /// GSS_KRB5_NT_PRINCIPAL_NAME - Kerberos principal name
    pub static GSS_KRB5_NT_PRINCIPAL_NAME: GssOid;
}

// =============================================================================
// Status type constants for gss_display_status
// =============================================================================

/// Major status (calling/routine error)
pub const GSS_C_GSS_CODE: i32 = 1;

/// Minor status (mechanism-specific)
pub const GSS_C_MECH_CODE: i32 = 2;

// =============================================================================
// Helper Functions
// =============================================================================

/// Extracts a human-readable error message from GSSAPI status codes.
///
/// # Safety
///
/// Calls unsafe FFI functions.
pub fn get_gssapi_error(major: GssOmUint32, minor: GssOmUint32) -> String {
    let mut messages = Vec::new();

    // Get major status message
    if let Some(msg) = get_status_message(major, GSS_C_GSS_CODE) {
        messages.push(format!("Major: {}", msg));
    }

    // Get minor status message (mechanism-specific)
    if let Some(msg) = get_status_message(minor, GSS_C_MECH_CODE).filter(|_| minor != 0) {
        messages.push(format!("Minor: {}", msg));
    }

    if messages.is_empty() {
        format!(
            "GSSAPI error (major=0x{:08X}, minor=0x{:08X})",
            major, minor
        )
    } else {
        messages.join("; ")
    }
}

/// Gets a single status message from GSSAPI.
fn get_status_message(status: GssOmUint32, status_type: i32) -> Option<String> {
    let mut minor_status: GssOmUint32 = 0;
    let mut message_context: GssOmUint32 = 0;
    let mut status_string = GssBufferDesc::default();
    let mut messages = Vec::new();

    loop {
        let major = unsafe {
            gss_display_status(
                &mut minor_status,
                status,
                status_type,
                GSS_C_NO_OID,
                &mut message_context,
                &mut status_string,
            )
        };

        if major != GSS_S_COMPLETE {
            break;
        }

        if status_string.length > 0 && !status_string.value.is_null() {
            let msg = unsafe {
                let slice = std::slice::from_raw_parts(
                    status_string.value as *const u8,
                    status_string.length,
                );
                String::from_utf8_lossy(slice).to_string()
            };
            messages.push(msg);

            // Release the buffer
            unsafe {
                gss_release_buffer(&mut minor_status, &mut status_string);
            }
        }

        // If message_context is 0, we've retrieved all messages
        if message_context == 0 {
            break;
        }
    }

    if messages.is_empty() {
        None
    } else {
        Some(messages.join(" "))
    }
}

/// Checks if GSSAPI library is available.
///
/// This is a compile-time check since we're statically linking.
/// For dynamic loading, we would use dlopen here.
pub fn is_gssapi_available() -> bool {
    // If this code compiles and runs, gssapi is available
    true
}

/// Checks if a valid Kerberos ticket is available.
///
/// Attempts to acquire default credentials to verify a TGT exists.
pub fn has_valid_credentials() -> bool {
    let mut minor_status: GssOmUint32 = 0;
    let mut cred_handle: GssCredIdT = ptr::null_mut();
    let mut time_rec: GssOmUint32 = 0;

    // Try to acquire default credentials (initiator)
    let major = unsafe {
        gss_acquire_cred(
            &mut minor_status,
            ptr::null_mut(), // GSS_C_NO_NAME - use default identity
            0,               // GSS_C_INDEFINITE
            ptr::null_mut(), // GSS_C_NO_OID_SET - default mechanisms
            1,               // GSS_C_INITIATE
            &mut cred_handle,
            ptr::null_mut(),
            &mut time_rec,
        )
    };

    // Clean up if we got a credential
    if !cred_handle.is_null() {
        unsafe {
            gss_release_cred(&mut minor_status, &mut cred_handle);
        }
    }

    major == GSS_S_COMPLETE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gss_buffer_desc_default() {
        let buf = GssBufferDesc::default();
        assert_eq!(buf.length, 0);
        assert!(buf.value.is_null());
    }

    #[test]
    fn test_gss_buffer_desc_from_str() {
        let spn = "MSSQLSvc/server:1433";
        let buf = GssBufferDesc::from_str(spn);
        assert_eq!(buf.length, spn.len());
        assert!(!buf.value.is_null());
    }

    #[test]
    fn test_gss_buffer_desc_from_slice() {
        let data = [0xDE, 0xAD, 0xBE, 0xEF];
        let buf = GssBufferDesc::from_slice(&data);
        assert_eq!(buf.length, 4);
        assert!(!buf.value.is_null());
    }

    #[test]
    fn test_status_codes() {
        // Verify our constants match expected values
        assert_eq!(GSS_S_COMPLETE, 0);
        assert_eq!(GSS_S_CONTINUE_NEEDED, 1);
        assert_eq!(GSS_C_MUTUAL_FLAG, 2);
        assert_eq!(GSS_C_DELEG_FLAG, 1);
    }
}
