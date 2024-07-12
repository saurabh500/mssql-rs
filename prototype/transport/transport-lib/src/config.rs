enum TranstortType {
    Tcp,
}

uint_enum! {
    /// The configured encryption level specifying if encryption is required
    #[repr(u8)]
    pub enum EncryptionLevel {
        /// Only use encryption for the login procedure
        Off = 0,
        /// Encrypt everything if possible
        On = 1,
        /// Do not encrypt anything
        NotSupported = 2,
        /// Encrypt everything and fail if not possible
        Required = 3,
    }
}

/// The `Config` struct contains configuration properties
/// for the parser to create a connection.
pub struct Config {
    host: String,
    user: String,
    password: String,
    transport_type: TranstortType,
}

impl Config {
    pub fn new(host: impl ToString, user: impl ToString, password: impl ToString) -> Self {
        Self {
            host: host.to_string(),
            user: user.to_string(),
            password: password.to_string(),
            transport_type: TranstortType::Tcp,
        }
    }

    pub(crate) fn get_host(&self) -> &str {
        &self.host
    }

    pub(crate) fn get_user(&self) -> &str {
        &self.user
    }

    pub(crate) fn get_password(&self) -> &str {
        &self.password
    }

    pub(crate) fn is_tcp(&self) -> bool {
        matches!(self.transport_type, TranstortType::Tcp)
    }
}
