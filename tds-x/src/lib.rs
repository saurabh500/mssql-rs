#![allow(dead_code)]
pub mod connection;
pub mod connection_provider;
pub mod core;
pub(crate) mod datatypes;
pub mod handler;
pub mod message;
pub mod query;
pub mod read_write;
pub mod token;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
