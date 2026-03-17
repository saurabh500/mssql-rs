// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(dead_code)]
//! SQL Server Resolution Protocol (SSRP) stub implementation
//!
//! This module provides stub implementations for SSRP functionality,
//! which is used to discover SQL Server instance information including:
//! - Instance ports
//! - Available protocols
//! - Dedicated Admin Connection (DAC) ports
//!
//! SSRP communicates with SQL Server Browser service (UDP port 1434)
//! to resolve named instances to their actual connection endpoints.
//!
//! ## Current Status
//! All functions are stubs that return NotImplemented errors.
//! Implementation is tracked separately.

use crate::connection::client_context::TransportContext;
use crate::core::TdsResult;
use crate::error::Error;

/// SSRP query response containing instance information
#[derive(Debug, Clone)]
pub struct SsrpInstanceInfo {
    /// Instance name
    pub instance_name: String,
    /// Protocol (tcp, np, etc.)
    pub protocol: String,
    /// TCP port (if protocol is tcp)
    pub tcp_port: Option<u16>,
    /// Named pipe path (if protocol is np)
    pub pipe_path: Option<String>,
}

/// Query SQL Server Browser for instance information
///
/// This function sends a CLNT_UCAST_INST packet to SQL Server Browser
/// on UDP port 1434 to discover the connection details for a named instance.
///
/// # Arguments
/// * `server` - Server name or IP address
/// * `instance` - Instance name
///
/// # Returns
/// List of available transport contexts for the instance
///
/// # ODBC Reference
/// Equivalent to `SSRP::SsrpGetInfo()` in /Sql/Common/DK/sni/src/ssrp.cpp
pub async fn get_instance_info(_server: &str, _instance: &str) -> TdsResult<Vec<SsrpInstanceInfo>> {
    // TODO: Implement SSRP query
    // 1. Create UDP socket
    // 2. Send CLNT_UCAST_INST request to server:1434
    // 3. Parse SVR_RESP response
    // 4. Extract protocol/port/pipe information
    // 5. Build list of SsrpInstanceInfo

    Err(Error::ProtocolError(
        "SSRP (SQL Server Browser queries) is not yet implemented. \
         Please specify an explicit protocol (tcp:server,port) or use default instance."
            .to_string(),
    ))
}

/// Query SQL Server Browser for Dedicated Admin Connection (DAC) port
///
/// DAC uses a special port (usually different from the default instance port)
/// that is discovered via SSRP query with the CLNT_UCAST_DAC packet type.
///
/// # Arguments
/// * `server` - Server name or IP address
/// * `instance` - Instance name
///
/// # Returns
/// The DAC port number
///
/// # ODBC Reference
/// Equivalent to `SSRP::GetAdminPort()` in /Sql/Common/DK/sni/src/ssrp.cpp
pub async fn get_admin_port(_server: &str, _instance: &str) -> TdsResult<u16> {
    // TODO: Implement DAC port query
    // 1. Create UDP socket
    // 2. Send CLNT_UCAST_DAC request to server:1434
    // 3. Parse response to extract DAC port

    Err(Error::ProtocolError(
        "SSRP (SQL Server Browser queries) for DAC is not yet implemented. \
         Please specify an explicit port for admin connections."
            .to_string(),
    ))
}

/// Build protocol list from SSRP response
///
/// Converts SSRP instance information into an ordered list of
/// TransportContext variants that can be attempted for connection.
///
/// # Arguments
/// * `instance_info` - SSRP query results
/// * `server` - Server name for building contexts
/// * `instance` - Instance name
///
/// # Returns
/// Ordered list of transport contexts to try
pub fn build_transport_list(
    instance_info: Vec<SsrpInstanceInfo>,
    server: &str,
    _instance: &str,
) -> Vec<TransportContext> {
    let mut transports = Vec::new();

    for info in instance_info {
        match info.protocol.as_str() {
            "tcp" => {
                if let Some(port) = info.tcp_port {
                    transports.push(TransportContext::Tcp {
                        host: server.to_string(),
                        port,
                        instance_name: None,
                    });
                }
            }
            "np" => {
                if let Some(pipe) = info.pipe_path {
                    transports.push(TransportContext::NamedPipe { pipe_name: pipe });
                }
            }
            _ => {
                // Unknown protocol, skip
            }
        }
    }

    transports
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_instance_info_not_implemented() {
        let result = get_instance_info("localhost", "SQLEXPRESS").await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not yet implemented")
        );
    }

    #[tokio::test]
    async fn test_get_admin_port_not_implemented() {
        let result = get_admin_port("localhost", "MSSQLSERVER").await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not yet implemented")
        );
    }

    #[test]
    fn test_build_transport_list() {
        let info = vec![
            SsrpInstanceInfo {
                instance_name: "INST1".to_string(),
                protocol: "tcp".to_string(),
                tcp_port: Some(1433),
                pipe_path: None,
            },
            SsrpInstanceInfo {
                instance_name: "INST1".to_string(),
                protocol: "np".to_string(),
                tcp_port: None,
                pipe_path: Some(r"\\.\pipe\MSSQL$INST1\sql\query".to_string()),
            },
        ];

        let transports = build_transport_list(info, "localhost", "INST1");
        assert_eq!(transports.len(), 2);
    }
}
