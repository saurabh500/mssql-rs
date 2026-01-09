// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Data source string parser for SQL Server connections
//!
//! This module implements the SQL Network Interface (SNI) layer's connection string
//! parsing logic, compatible with ODBC driver behavior.

use crate::core::TdsResult;
use crate::error::Error;

/// Represents a parsed data source string with all connection parameters
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedDataSource {
    /// Protocol name (tcp, np, lpc, admin) or empty for auto-detection
    pub protocol_name: String,
    /// Server name (resolved, e.g., localhost or actual hostname)
    pub server_name: String,
    /// Original server name as provided by user
    pub original_server_name: String,
    /// Instance name (e.g., SQLEXPRESS) or empty for default instance
    pub instance_name: String,
    /// Protocol parameter (port number for TCP, pipe path for Named Pipes)
    pub protocol_parameter: String,
    /// Alias used for connection caching (server\instance format)
    pub alias: String,
    /// Whether this connection can use the connection cache
    pub can_use_cache: bool,
    /// Whether the instance name follows standard naming convention
    pub standard_instance_name: bool,
    /// Whether parallel connection is requested (MultiSubnetFailover)
    pub parallel_connect: bool,
}

/// Protocol types for SQL Server connections
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolType {
    /// TCP/IP protocol
    Tcp,
    /// Named Pipes protocol
    NamedPipe,
    /// Shared Memory protocol (local only, Windows)
    SharedMemory,
    /// Dedicated Admin Connection
    Admin,
    /// Auto-detect (no protocol specified)
    Auto,
}

impl ParsedDataSource {
    /// Creates a new ParsedDataSource with default values
    pub fn new() -> Self {
        Self {
            protocol_name: String::new(),
            server_name: String::new(),
            original_server_name: String::new(),
            instance_name: String::new(),
            protocol_parameter: String::new(),
            alias: String::new(),
            can_use_cache: true,
            standard_instance_name: true,
            parallel_connect: false,
        }
    }

    /// Parse a data source string into structured connection parameters
    ///
    /// # Arguments
    /// * `datasource` - The data source string (e.g., "tcp:server,1433", "server\instance")
    /// * `parallel_connect` - Whether MultiSubnetFailover is enabled
    ///
    /// # Examples
    /// ```
    /// use mssql_tds::connection::datasource_parser::ParsedDataSource;
    ///
    /// let parsed = ParsedDataSource::parse("tcp:myserver,1433", false)?;
    /// assert_eq!(parsed.protocol_name, "tcp");
    /// assert_eq!(parsed.server_name, "myserver");
    /// assert_eq!(parsed.protocol_parameter, "1433");
    /// ```
    pub fn parse(datasource: &str, parallel_connect: bool) -> TdsResult<Self> {
        let mut result = Self::new();
        result.parallel_connect = parallel_connect;

        // Step 1: Normalize the string (lowercase, trim whitespace)
        let normalized = datasource.trim().to_lowercase();
        if normalized.is_empty() {
            return Err(Error::ProtocolError(
                "Data source string cannot be empty".to_string(),
            ));
        }

        // Step 2: Check for LocalDB format (Windows only)
        #[cfg(windows)]
        if normalized.starts_with("(localdb)\\") || normalized.starts_with("(localdb)/") {
            return Self::parse_localdb(&normalized);
        }

        // Step 3: Parse protocol prefix (tcp:, np:, lpc:, admin:)
        let after_protocol = Self::parse_protocol(&normalized, &mut result)?;

        // Step 4: Check for named pipe (starts with \\)
        if after_protocol.starts_with("\\\\") {
            return Self::parse_named_pipe(after_protocol, &mut result);
        }

        // Step 5: Parse parameter (port) - look for comma
        let after_parameter = Self::parse_parameter(after_protocol, &mut result)?;

        // Step 6: Parse instance name - look for backslash
        let after_instance = Self::parse_instance(after_parameter, &mut result)?;

        // Step 7: Parse and resolve server name
        Self::parse_server(after_instance, &mut result)?;

        // Step 8: Validate protocol constraints
        Self::validate_protocol(&mut result)?;

        // Step 9: Build alias and determine cache eligibility
        Self::build_alias(&mut result);

        Ok(result)
    }

    /// Parse protocol prefix (tcp:, np:, lpc:, admin:)
    fn parse_protocol<'a>(input: &'a str, result: &mut ParsedDataSource) -> TdsResult<&'a str> {
        // Look for colon delimiter
        if let Some(colon_pos) = input.find(':') {
            // Check if this is IPv6 address (multiple colons)
            let before_colon = &input[..colon_pos];

            // IPv6 addresses contain :: or multiple colons
            let is_ipv6 = input.contains("::") || input.matches(':').count() > 1;

            if !is_ipv6 && !before_colon.is_empty() {
                // Valid protocol prefix
                let protocol = before_colon.trim();
                if matches!(protocol, "tcp" | "np" | "lpc" | "admin") {
                    result.protocol_name = protocol.to_string();
                    return Ok(input[colon_pos + 1..].trim_start());
                }
            }
        }

        // No protocol or IPv6 address - return as-is
        Ok(input)
    }

    /// Parse named pipe path (\\server\pipe\...)
    fn parse_named_pipe(input: &str, result: &mut ParsedDataSource) -> TdsResult<ParsedDataSource> {
        // Named pipe format: \\server\pipe\...
        if !input.starts_with("\\\\") {
            return Err(Error::ProtocolError(
                "Invalid named pipe path".to_string(),
            ));
        }

        // Extract server name (between \\ and next \)
        let after_slashes = &input[2..];
        let server_end = after_slashes.find('\\').ok_or_else(|| {
            Error::ProtocolError("Invalid named pipe path - no server specified".to_string())
        })?;

        let server = &after_slashes[..server_end];
        if server.is_empty() {
            return Err(Error::ProtocolError(
                "Invalid named pipe path - blank server".to_string(),
            ));
        }

        // Store the full pipe path
        result.protocol_parameter = input.to_string();
        result.protocol_name = "np".to_string();

        // Resolve server name (. becomes localhost)
        result.original_server_name = server.to_string();
        result.server_name = if server == "." {
            "localhost".to_string()
        } else {
            server.to_string()
        };

        // Parse instance from pipe path
        let pipe_path = &after_slashes[server_end..];

        // Standard default instance: \pipe\sql\query
        if pipe_path == "\\pipe\\sql\\query" {
            result.instance_name = String::new(); // default instance
            result.standard_instance_name = true;
        }
        // Standard named instance: \pipe\MSSQL$instance\sql\query
        else if pipe_path.starts_with("\\pipe\\mssql$") && pipe_path.ends_with("\\sql\\query") {
            let start = "\\pipe\\mssql$".len();
            let end = pipe_path.len() - "\\sql\\query".len();
            result.instance_name = pipe_path[start..end].to_string();
            result.standard_instance_name = true;
        }
        // Non-standard pipe
        else if pipe_path.starts_with("\\pipe\\") {
            // Use "pipe<rest_of_path>" format
            let custom_path = &pipe_path["\\pipe\\".len()..];
            result.instance_name = format!("pipe{}", custom_path);
            result.standard_instance_name = false;
        } else {
            return Err(Error::ProtocolError(format!(
                "Invalid named pipe path: {}",
                input
            )));
        }

        result.can_use_cache = false;
        Ok(result.clone())
    }

    /// Parse protocol parameter (port for TCP)
    fn parse_parameter<'a>(
        input: &'a str,
        result: &mut ParsedDataSource,
    ) -> TdsResult<&'a str> {
        // Look for comma separator
        if let Some(comma_pos) = input.find(',') {
            let parameter = input[comma_pos + 1..].trim();

            // If no protocol specified, default to TCP
            if result.protocol_name.is_empty() {
                result.protocol_name = "tcp".to_string();
            }

            // Port is only valid for TCP protocol
            if result.protocol_name != "tcp" {
                return Err(Error::ProtocolError(format!(
                    "Port parameter only valid for TCP protocol, got: {}",
                    result.protocol_name
                )));
            }

            // Store port number, strip any instance name after it
            let port = if let Some(backslash_pos) = parameter.find('\\') {
                &parameter[..backslash_pos]
            } else {
                parameter
            };

            result.protocol_parameter = port.to_string();
            result.can_use_cache = false;

            // Return the part before comma (server and possibly instance)
            return Ok(input[..comma_pos].trim_end());
        }

        Ok(input)
    }

    /// Parse instance name (after backslash)
    fn parse_instance<'a>(input: &'a str, result: &mut ParsedDataSource) -> TdsResult<&'a str> {
        // Look for backslash separator
        if let Some(backslash_pos) = input.find('\\') {
            let instance = input[backslash_pos + 1..].trim();

            // Port takes priority - if port is already set, ignore instance
            if result.protocol_parameter.is_empty() {
                // Validate: "mssqlserver" is reserved and invalid
                if instance.eq_ignore_ascii_case("mssqlserver") {
                    return Err(Error::ProtocolError(
                        "Instance name 'MSSQLSERVER' is reserved".to_string(),
                    ));
                }

                result.instance_name = instance.to_string();
            }

            // Return server part (before backslash)
            return Ok(input[..backslash_pos].trim_end());
        }

        Ok(input)
    }

    /// Parse and resolve server name
    fn parse_server(input: &str, result: &mut ParsedDataSource) -> TdsResult<()> {
        let server = input.trim();
        result.original_server_name = server.to_string();

        // Check if this is a local host alias
        let is_local = server == "."
            || server == "(local)"
            || server.eq_ignore_ascii_case("localhost")
            || Self::is_computer_name(server);

        if is_local {
            // For admin (DAC) protocol, always use "localhost"
            if result.protocol_name == "admin" {
                result.server_name = "localhost".to_string();
            } else {
                // For other protocols, resolve to actual computer name
                result.server_name = Self::get_computer_name().unwrap_or_else(|| server.to_string());
            }
        } else {
            result.server_name = server.to_string();
        }

        Ok(())
    }

    /// Validate protocol-specific constraints
    fn validate_protocol(result: &mut ParsedDataSource) -> TdsResult<()> {
        // LPC (Shared Memory) requires local server
        if result.protocol_name == "lpc" {
            let is_local = result.original_server_name == "."
                || result.original_server_name == "(local)"
                || result.original_server_name.eq_ignore_ascii_case("localhost")
                || Self::is_computer_name(&result.original_server_name);

            if !is_local {
                return Err(Error::ProtocolError(
                    "Shared Memory (lpc) protocol requires local server".to_string(),
                ));
            }
        }

        // Parallel connect (MultiSubnetFailover) has restrictions
        if result.parallel_connect {
            // Cannot use np, lpc, or admin with parallel connect
            if matches!(result.protocol_name.as_str(), "np" | "lpc" | "admin") {
                return Err(Error::ProtocolError(format!(
                    "Protocol '{}' is incompatible with parallel connect (MultiSubnetFailover)",
                    result.protocol_name
                )));
            }

            // If no protocol specified, default to TCP
            if result.protocol_name.is_empty() {
                result.protocol_name = "tcp".to_string();
            }
        }

        Ok(())
    }

    /// Build alias and determine cache eligibility
    fn build_alias(result: &mut ParsedDataSource) {
        // Build alias as server\instance or just server
        if result.instance_name.is_empty() {
            result.alias = result.server_name.clone();
        } else {
            result.alias = format!("{}\\{}", result.server_name, result.instance_name);
        }

        // Cache is disabled if any of these conditions are true:
        // - Named pipe path specified
        // - Explicit port specified
        // - LPC/admin protocol with instance
        // - Parallel connect enabled
        // - Non-standard instance name

        if !result.protocol_parameter.is_empty() {
            result.can_use_cache = false;
        }

        if matches!(result.protocol_name.as_str(), "lpc" | "admin") && !result.instance_name.is_empty() {
            result.can_use_cache = false;
        }

        if result.parallel_connect {
            result.can_use_cache = false;
        }

        if !result.standard_instance_name {
            result.can_use_cache = false;
        }
    }

    /// Parse LocalDB connection string (Windows only)
    #[cfg(windows)]
    fn parse_localdb(input: &str) -> TdsResult<ParsedDataSource> {
        // Format: (localdb)\instancename or (localdb)/instancename
        let instance_start = if input.starts_with("(localdb)\\") {
            "(localdb)\\".len()
        } else if input.starts_with("(localdb)/") {
            "(localdb)/".len()
        } else {
            return Err(Error::ProtocolError(
                "Invalid LocalDB format".to_string(),
            ));
        };

        let instance_name = input[instance_start..].trim();
        if instance_name.is_empty() {
            return Err(Error::ProtocolError(
                "LocalDB instance name cannot be empty".to_string(),
            ));
        }

        // LocalDB connections will be resolved to named pipes later
        // For now, return a placeholder that indicates LocalDB
        let mut result = ParsedDataSource::new();
        result.protocol_name = "localdb".to_string();
        result.server_name = input.to_string(); // Store original for later resolution
        result.original_server_name = input.to_string();
        result.instance_name = instance_name.to_string();
        result.can_use_cache = false; // LocalDB connections are never cached
        result.alias = input.to_string();

        Ok(result)
    }

    /// Check if the given name matches the computer name
    fn is_computer_name(name: &str) -> bool {
        // Get computer name and compare
        if let Some(computer_name) = Self::get_computer_name() {
            name.eq_ignore_ascii_case(&computer_name)
        } else {
            false
        }
    }

    /// Get the local computer name
    fn get_computer_name() -> Option<String> {
        hostname::get()
            .ok()
            .and_then(|name| name.into_string().ok())
    }

    /// Get the protocol type from the parsed data source
    pub fn get_protocol_type(&self) -> ProtocolType {
        match self.protocol_name.as_str() {
            "tcp" => ProtocolType::Tcp,
            "np" => ProtocolType::NamedPipe,
            "lpc" => ProtocolType::SharedMemory,
            "admin" => ProtocolType::Admin,
            "" => ProtocolType::Auto,
            #[cfg(windows)]
            "localdb" => ProtocolType::NamedPipe, // LocalDB uses named pipes
            _ => ProtocolType::Auto,
        }
    }

    /// Check if this represents a local connection
    pub fn is_local(&self) -> bool {
        self.original_server_name == "."
            || self.original_server_name == "(local)"
            || self.original_server_name.eq_ignore_ascii_case("localhost")
            || self.original_server_name == "127.0.0.1"
            || self.original_server_name == "::1"
            || Self::is_computer_name(&self.original_server_name)
    }

    /// Check if SSRP (SQL Server Resolution Protocol) is needed
    ///
    /// SSRP is used when:
    /// - Named instance is specified
    /// - No explicit port is provided
    /// - No explicit protocol details are given
    pub fn needs_ssrp(&self) -> bool {
        !self.instance_name.is_empty()
            && self.protocol_parameter.is_empty()
            && self.protocol_name.is_empty()
    }
}

impl Default for ParsedDataSource {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tcp_with_port() {
        let parsed = ParsedDataSource::parse("tcp:myserver,1433", false).unwrap();
        assert_eq!(parsed.protocol_name, "tcp");
        assert_eq!(parsed.server_name, "myserver");
        assert_eq!(parsed.protocol_parameter, "1433");
        assert_eq!(parsed.instance_name, "");
        assert_eq!(parsed.alias, "myserver");
        assert!(!parsed.can_use_cache);
    }

    #[test]
    fn test_named_instance() {
        let parsed = ParsedDataSource::parse("myserver\\SQLEXPRESS", false).unwrap();
        assert_eq!(parsed.protocol_name, "");
        assert_eq!(parsed.server_name, "myserver");
        assert_eq!(parsed.instance_name, "SQLEXPRESS");
        assert_eq!(parsed.alias, "myserver\\SQLEXPRESS");
        assert!(parsed.can_use_cache);
    }

    #[test]
    fn test_named_pipe_default_instance() {
        let parsed = ParsedDataSource::parse("np:\\\\myserver\\pipe\\sql\\query", false).unwrap();
        assert_eq!(parsed.protocol_name, "np");
        assert_eq!(parsed.server_name, "myserver");
        assert_eq!(parsed.instance_name, "");
        assert_eq!(parsed.protocol_parameter, "\\\\myserver\\pipe\\sql\\query");
        assert!(!parsed.can_use_cache);
        assert!(parsed.standard_instance_name);
    }

    #[test]
    fn test_named_pipe_named_instance() {
        let parsed =
            ParsedDataSource::parse("\\\\myserver\\pipe\\mssql$inst1\\sql\\query", false).unwrap();
        assert_eq!(parsed.protocol_name, "np");
        assert_eq!(parsed.server_name, "myserver");
        assert_eq!(parsed.instance_name, "inst1");
        assert!(parsed.standard_instance_name);
    }

    #[test]
    fn test_named_pipe_non_standard() {
        let parsed =
            ParsedDataSource::parse("\\\\myserver\\pipe\\custom\\myapp\\sql", false).unwrap();
        assert_eq!(parsed.protocol_name, "np");
        assert_eq!(parsed.instance_name, "pipecustom\\myapp\\sql");
        assert!(!parsed.standard_instance_name);
    }

    #[test]
    fn test_local_server_resolution() {
        let parsed = ParsedDataSource::parse("(local)\\SQLEXPRESS", false).unwrap();
        assert_eq!(parsed.original_server_name, "(local)");
        assert_eq!(parsed.instance_name, "SQLEXPRESS");
        assert!(parsed.is_local());
    }

    #[test]
    fn test_port_with_instance_port_wins() {
        let parsed = ParsedDataSource::parse("myserver\\INST1,1433", false).unwrap();
        assert_eq!(parsed.protocol_name, "tcp");
        assert_eq!(parsed.protocol_parameter, "1433");
        assert_eq!(parsed.instance_name, ""); // Instance ignored when port specified
    }

    #[test]
    fn test_admin_protocol() {
        let parsed = ParsedDataSource::parse("admin:localhost", false).unwrap();
        assert_eq!(parsed.protocol_name, "admin");
        assert_eq!(parsed.server_name, "localhost");
    }

    #[test]
    fn test_lpc_local_only() {
        // LPC with local server should succeed
        let parsed = ParsedDataSource::parse("lpc:.", false).unwrap();
        assert_eq!(parsed.protocol_name, "lpc");

        // LPC with remote server should fail
        let result = ParsedDataSource::parse("lpc:remoteserver", false);
        assert!(result.is_err());
    }

    #[test]
    fn test_parallel_connect_restrictions() {
        // Parallel with TCP should work
        let parsed = ParsedDataSource::parse("tcp:myserver,1433", true).unwrap();
        assert_eq!(parsed.protocol_name, "tcp");
        assert!(parsed.parallel_connect);

        // Parallel with NP should fail
        let result = ParsedDataSource::parse("np:\\\\server\\pipe\\sql\\query", true);
        assert!(result.is_err());

        // Parallel with no protocol should default to TCP
        let parsed = ParsedDataSource::parse("myserver", true).unwrap();
        assert_eq!(parsed.protocol_name, "tcp");
    }

    #[test]
    fn test_ipv6_address() {
        // IPv6 address should not be confused with protocol prefix
        let parsed = ParsedDataSource::parse("::1", false).unwrap();
        assert_eq!(parsed.protocol_name, "");
        assert_eq!(parsed.server_name, "::1");
    }

    #[test]
    fn test_reserved_instance_name() {
        // "MSSQLSERVER" is reserved
        let result = ParsedDataSource::parse("myserver\\MSSQLSERVER", false);
        assert!(result.is_err());
    }

    #[test]
    fn test_needs_ssrp() {
        // Named instance without port needs SSRP
        let parsed = ParsedDataSource::parse("myserver\\SQLEXPRESS", false).unwrap();
        assert!(parsed.needs_ssrp());

        // With port, no SSRP needed
        let parsed = ParsedDataSource::parse("myserver,1433", false).unwrap();
        assert!(!parsed.needs_ssrp());

        // No instance, no SSRP needed
        let parsed = ParsedDataSource::parse("myserver", false).unwrap();
        assert!(!parsed.needs_ssrp());
    }

    #[test]
    #[cfg(windows)]
    fn test_localdb_parsing() {
        let parsed = ParsedDataSource::parse("(localdb)\\MSSQLLocalDB", false).unwrap();
        assert_eq!(parsed.protocol_name, "localdb");
        assert_eq!(parsed.instance_name, "MSSQLLocalDB");
        assert!(!parsed.can_use_cache);

        // Forward slash also supported
        let parsed = ParsedDataSource::parse("(localdb)/v11.0", false).unwrap();
        assert_eq!(parsed.instance_name, "v11.0");

        // Empty instance name should fail
        let result = ParsedDataSource::parse("(localdb)\\", false);
        assert!(result.is_err());
    }

    #[test]
    fn test_case_insensitivity() {
        let parsed = ParsedDataSource::parse("TCP:MyServer,1433", false).unwrap();
        assert_eq!(parsed.protocol_name, "tcp"); // Normalized to lowercase

        let parsed = ParsedDataSource::parse("MyServer\\SqlExpress", false).unwrap();
        assert_eq!(parsed.instance_name, "sqlexpress"); // Normalized to lowercase
    }

    #[test]
    fn test_whitespace_trimming() {
        let parsed = ParsedDataSource::parse("  tcp:myserver , 1433  ", false).unwrap();
        assert_eq!(parsed.protocol_name, "tcp");
        assert_eq!(parsed.server_name, "myserver");
        assert_eq!(parsed.protocol_parameter, "1433");
    }
}

