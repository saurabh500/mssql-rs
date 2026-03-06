// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::collections::HashMap;

use mssql_tds::connection::client_context::{ClientContext, TdsAuthenticationMethod};
use mssql_tds::connection_provider::tds_connection_provider::TdsConnectionProvider;
use mssql_tds::core::{CancelHandle, EncryptionSetting};

use crate::client::Client;
use crate::error::{Error, Result};

/// Parse an ODBC-style connection string into key-value pairs.
///
/// Format: `Key1=Value1;Key2=Value2;...`
/// Keys are case-insensitive. Values may be enclosed in braces `{...}`.
fn parse_connection_string(conn_str: &str) -> Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    let mut remaining = conn_str.trim();

    while !remaining.is_empty() {
        // Find the key
        let eq_pos = remaining.find('=').ok_or_else(|| {
            Error::ConnectionStringInvalid(format!(
                "expected '=' after key near: '{}'",
                &remaining[..remaining.len().min(30)]
            ))
        })?;
        let key = remaining[..eq_pos].trim().to_lowercase();
        remaining = &remaining[eq_pos + 1..];

        // Parse value (possibly brace-quoted)
        let trimmed = remaining.trim_start();
        let (value, rest) = if let Some(inner) = trimmed.strip_prefix('{') {
            // Brace-quoted value
            let close = inner
                .find('}')
                .ok_or_else(|| Error::ConnectionStringInvalid("unclosed brace in value".into()))?;
            let val = &inner[..close];
            let after = &inner[close + 1..];
            // Skip trailing semicolon
            let after = after.trim_start().strip_prefix(';').unwrap_or(after);
            (val.to_string(), after)
        } else {
            // Unquoted value — terminated by ';' or end of string
            match trimmed.find(';') {
                Some(semi) => {
                    let val = trimmed[..semi].trim_end().to_string();
                    (val, &trimmed[semi + 1..])
                }
                None => (trimmed.trim_end().to_string(), ""),
            }
        };

        if key.is_empty() {
            return Err(Error::ConnectionStringInvalid("empty key".into()));
        }
        map.insert(key, value);
        remaining = rest.trim_start();
    }

    Ok(map)
}

/// Map parsed connection string keys to `ClientContext` fields (research R1).
fn apply_to_context(ctx: &mut ClientContext, props: &HashMap<String, String>) -> Result<()> {
    for (key, value) in props {
        match key.as_str() {
            "server" => {
                // Already handled during context creation via data_source
            }
            "database" | "initial catalog" => {
                ctx.database = value.clone();
            }
            "user id" | "uid" => {
                ctx.user_name = value.clone();
            }
            "password" | "pwd" => {
                ctx.password = value.clone();
            }
            "encrypt" => {
                ctx.encryption_options.mode = match value.to_lowercase().as_str() {
                    "yes" | "true" | "mandatory" => EncryptionSetting::On,
                    "no" | "false" | "optional" => EncryptionSetting::PreferOff,
                    "strict" => EncryptionSetting::Strict,
                    other => {
                        return Err(Error::ConnectionStringInvalid(format!(
                            "invalid encrypt value: '{other}'"
                        )));
                    }
                };
            }
            "trustservercertificate" => {
                ctx.encryption_options.trust_server_certificate =
                    value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("yes");
            }
            "connection timeout" | "connect timeout" => {
                let secs: u32 = value.parse().map_err(|_| {
                    Error::ConnectionStringInvalid(format!("invalid connection timeout: '{value}'"))
                })?;
                ctx.connect_timeout = secs;
            }
            "command timeout" => {
                // Command timeout is stored per-query, not on ClientContext.
                // Handled separately — stored in Client struct.
            }
            "application name" | "app" => {
                ctx.application_name = value.clone();
            }
            "packet size" => {
                let ps: u16 = value.parse().map_err(|_| {
                    Error::ConnectionStringInvalid(format!("invalid packet size: '{value}'"))
                })?;
                ctx.packet_size = ps;
            }
            "authentication" => {
                ctx.tds_authentication_method = match value.to_lowercase().as_str() {
                    "sqlpassword" | "sql password" => TdsAuthenticationMethod::Password,
                    "activedirectorypassword" => TdsAuthenticationMethod::ActiveDirectoryPassword,
                    "activedirectoryinteractive" => {
                        TdsAuthenticationMethod::ActiveDirectoryInteractive
                    }
                    "activedirectoryserviceprincipal" => {
                        TdsAuthenticationMethod::ActiveDirectoryServicePrincipal
                    }
                    "activedirectorymanagedidentity" | "activedirectorymsi" => {
                        TdsAuthenticationMethod::ActiveDirectoryManagedIdentity
                    }
                    "activedirectorydefault" => TdsAuthenticationMethod::ActiveDirectoryDefault,
                    "activedirectoryintegrated" => {
                        TdsAuthenticationMethod::ActiveDirectoryIntegrated
                    }
                    other => {
                        return Err(Error::ConnectionStringInvalid(format!(
                            "unknown authentication method: '{other}'"
                        )));
                    }
                };
            }
            "access token" => {
                ctx.access_token = Some(value.clone());
            }
            _ => {
                return Err(Error::ConnectionStringInvalid(format!(
                    "unknown key: '{key}'"
                )));
            }
        }
    }
    Ok(())
}

impl Client {
    /// Connect to SQL Server using an ODBC-style connection string.
    ///
    /// Keys are case-insensitive and separated by semicolons. Standard keys:
    /// `Server`, `Database`, `User Id`, `Password`, `Encrypt`,
    /// `TrustServerCertificate`, `Connection Timeout`, `Command Timeout`,
    /// `Application Name`, `Packet Size`, `Authentication`, `Access Token`.
    pub async fn connect(connection_string: &str) -> Result<Client> {
        let props = parse_connection_string(connection_string)?;

        let server = props
            .get("server")
            .ok_or_else(|| Error::ConnectionStringInvalid("missing 'Server' key".into()))?;

        let mut ctx = ClientContext::with_data_source(server);
        apply_to_context(&mut ctx, &props)?;

        // Parse command timeout separately (not a ClientContext field)
        let command_timeout: Option<u32> = if let Some(val) = props.get("command timeout") {
            Some(val.parse().map_err(|_| {
                Error::ConnectionStringInvalid(format!("invalid command timeout: '{val}'"))
            })?)
        } else {
            None
        };

        let cancel_handle = CancelHandle::new();
        let provider = TdsConnectionProvider::new();
        let inner = provider
            .create_client(ctx, server, Some(&cancel_handle))
            .await?;

        Ok(Client {
            inner,
            cancel_handle,
            command_timeout,
            pending_rollback: false,
            pending_unprepare: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic() {
        let props =
            parse_connection_string("Server=localhost;Database=master;User Id=sa;Password=pass")
                .unwrap();
        assert_eq!(props.get("server").unwrap(), "localhost");
        assert_eq!(props.get("database").unwrap(), "master");
        assert_eq!(props.get("user id").unwrap(), "sa");
        assert_eq!(props.get("password").unwrap(), "pass");
    }

    #[test]
    fn parse_braces() {
        let props = parse_connection_string("Password={semi;colon};Server=host").unwrap();
        assert_eq!(props.get("password").unwrap(), "semi;colon");
        assert_eq!(props.get("server").unwrap(), "host");
    }

    #[test]
    fn parse_case_insensitive() {
        let props = parse_connection_string("SERVER=host;DaTaBaSe=db").unwrap();
        assert_eq!(props.get("server").unwrap(), "host");
        assert_eq!(props.get("database").unwrap(), "db");
    }

    #[test]
    fn parse_unknown_key_rejected() {
        let mut ctx = ClientContext::with_data_source("localhost");
        let mut props = HashMap::new();
        props.insert("unknownkey".to_string(), "value".to_string());
        let result = apply_to_context(&mut ctx, &props);
        assert!(result.is_err());
    }

    #[test]
    fn parse_empty_string() {
        let props = parse_connection_string("").unwrap();
        assert!(props.is_empty());
    }
}
