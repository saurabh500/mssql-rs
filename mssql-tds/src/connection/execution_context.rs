// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use crate::core::TdsResult;
use crate::message::login::EnvChangeProperties;
use crate::token::tokens::{EnvChangeContainer, EnvChangeToken, EnvChangeTokenSubType};
use tracing::{info, instrument};

/// Execution context tracks the state of the current connection session.
/// This includes transaction state, batch execution state, and environment changes.
#[derive(Debug)]
pub(crate) struct ExecutionContext {
    transaction_descriptor: u64,
    outstanding_requests: u32,
    has_open_batch: bool,
    has_open_result_set: bool,
    change_properties: EnvChangeProperties,
}

impl ExecutionContext {
    pub(crate) fn new() -> Self {
        Self {
            transaction_descriptor: 0,
            outstanding_requests: 1,
            has_open_batch: false,
            has_open_result_set: false,
            change_properties: EnvChangeProperties::default(),
        }
    }

    pub(crate) fn get_transaction_descriptor(&self) -> u64 {
        self.transaction_descriptor
    }

    pub(crate) fn get_outstanding_requests(&self) -> u32 {
        self.outstanding_requests
    }

    #[instrument(skip(self))]
    pub fn has_open_batch(&self) -> bool {
        self.has_open_batch
    }

    pub fn has_open_result_set(&self) -> bool {
        self.has_open_result_set
    }

    #[instrument(skip(self))]
    pub(crate) fn set_has_open_batch(&mut self, has_open_batch: bool) {
        self.has_open_batch = has_open_batch;
    }

    #[instrument(skip(self))]
    pub(crate) fn set_has_open_result_set(&mut self, has_open_result_set: bool) {
        self.has_open_result_set = has_open_result_set;
    }

    pub(crate) fn capture_change_property(
        &mut self,
        change_token: &EnvChangeToken,
    ) -> TdsResult<()> {
        let sub_type = change_token.sub_type;
        let change_type = &change_token.change_type;

        match &sub_type {
            EnvChangeTokenSubType::BeginTransaction
            | EnvChangeTokenSubType::CommitTransaction
            | EnvChangeTokenSubType::RollbackTransaction
            | EnvChangeTokenSubType::EnlistDtcTransaction
            | EnvChangeTokenSubType::DefectTransaction => {
                if let EnvChangeContainer::UInt64(u64_change) = change_type {
                    self.transaction_descriptor = *u64_change.new_value();
                    Ok(())
                } else {
                    Err(crate::error::Error::ProtocolError(format!(
                        "Expected UInt64 change container, but got: {change_token:?}",
                    )))
                }
            }
            EnvChangeTokenSubType::Database => {
                if let EnvChangeContainer::String(string_change) = change_type {
                    info!("Database change detected: {}", string_change.new_value());
                    self.change_properties.database = Some(string_change.new_value().clone());
                    Ok(())
                } else {
                    Err(crate::error::Error::ProtocolError(format!(
                        "Expected String change container, but got: {change_token:?}",
                    )))
                }
            }
            EnvChangeTokenSubType::Language => {
                if let EnvChangeContainer::String(string_change) = change_type {
                    self.change_properties.language =
                        Option::from(string_change.new_value().clone());
                    Ok(())
                } else {
                    Err(crate::error::Error::ProtocolError(format!(
                        "Expected String change container, but got: {change_token:?}",
                    )))
                }
            }
            EnvChangeTokenSubType::SqlCollation => {
                if let EnvChangeContainer::SqlCollation(collation_change) = change_type {
                    info!("Collation change detected: {:?}", collation_change);
                    self.change_properties.database_collation = *collation_change.new_value();
                    Ok(())
                } else {
                    Err(crate::error::Error::ProtocolError(format!(
                        "Expected Collation change container, but got: {change_token:?}",
                    )))
                }
            }
            EnvChangeTokenSubType::PacketSize => Err(crate::error::Error::ProtocolError(
                "packet_size change unexpected".to_string(),
            )),
            EnvChangeTokenSubType::CharacterSet => todo!(),
            EnvChangeTokenSubType::UnicodeDataSortingLocalId => todo!(),
            EnvChangeTokenSubType::UnicodeDataSortingComparisonFlags => todo!(),
            EnvChangeTokenSubType::DatabaseMirroringPartner => todo!(),
            EnvChangeTokenSubType::PromoteTransaction => todo!(),
            EnvChangeTokenSubType::TransactionManagerAddress => todo!(),
            EnvChangeTokenSubType::TransactionEnded => todo!(),
            EnvChangeTokenSubType::ResetConnection => todo!(),
            EnvChangeTokenSubType::UserInstanceName => todo!(),
            EnvChangeTokenSubType::Routing => todo!(),
            EnvChangeTokenSubType::Unknown(value) => {
                // Log unknown environment change subtypes but don't fail
                info!("Unknown environment change subtype: {}", value);
                Ok(())
            }
        }
    }
}

pub(crate) const ALREADY_EXECUTING_ERROR: &str = "There is an open batch on the current connection. It must be closed or fully consumed before executing another operation.";
