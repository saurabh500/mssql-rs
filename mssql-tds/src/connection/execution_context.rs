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
            EnvChangeTokenSubType::CharacterSet => {
                Err(crate::error::Error::UnimplementedFeature {
                    feature: "CharacterSet environment change".to_string(),
                    context: "capture_change_property".to_string(),
                })
            }
            EnvChangeTokenSubType::UnicodeDataSortingLocalId => {
                Err(crate::error::Error::UnimplementedFeature {
                    feature: "UnicodeDataSortingLocalId environment change".to_string(),
                    context: "capture_change_property".to_string(),
                })
            }
            EnvChangeTokenSubType::UnicodeDataSortingComparisonFlags => {
                Err(crate::error::Error::UnimplementedFeature {
                    feature: "UnicodeDataSortingComparisonFlags environment change".to_string(),
                    context: "capture_change_property".to_string(),
                })
            }
            EnvChangeTokenSubType::DatabaseMirroringPartner => {
                Err(crate::error::Error::UnimplementedFeature {
                    feature: "DatabaseMirroringPartner environment change".to_string(),
                    context: "capture_change_property".to_string(),
                })
            }
            EnvChangeTokenSubType::PromoteTransaction => {
                Err(crate::error::Error::UnimplementedFeature {
                    feature: "PromoteTransaction environment change".to_string(),
                    context: "capture_change_property".to_string(),
                })
            }
            EnvChangeTokenSubType::TransactionManagerAddress => {
                Err(crate::error::Error::UnimplementedFeature {
                    feature: "TransactionManagerAddress environment change".to_string(),
                    context: "capture_change_property".to_string(),
                })
            }
            EnvChangeTokenSubType::TransactionEnded => {
                Err(crate::error::Error::UnimplementedFeature {
                    feature: "TransactionEnded environment change".to_string(),
                    context: "capture_change_property".to_string(),
                })
            }
            EnvChangeTokenSubType::ResetConnection => {
                Err(crate::error::Error::UnimplementedFeature {
                    feature: "ResetConnection environment change".to_string(),
                    context: "capture_change_property".to_string(),
                })
            }
            EnvChangeTokenSubType::UserInstanceName => {
                Err(crate::error::Error::UnimplementedFeature {
                    feature: "UserInstanceName environment change".to_string(),
                    context: "capture_change_property".to_string(),
                })
            }
            EnvChangeTokenSubType::Routing => {
                Err(crate::error::Error::UnimplementedFeature {
                    feature: "Routing environment change".to_string(),
                    context: "capture_change_property".to_string(),
                })
            }
            EnvChangeTokenSubType::Unknown(value) => {
                // Log unknown environment change subtypes but don't fail
                info!("Unknown environment change subtype: {}", value);
                Ok(())
            }
        }
    }
}

pub(crate) const ALREADY_EXECUTING_ERROR: &str = "There is an open batch on the current connection. It must be closed or fully consumed before executing another operation.";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::tokens::{
        EnvChangeContainer, EnvChangeToken, EnvChangeTokenSubType, SqlCollation,
    };

    #[test]
    fn test_new_execution_context() {
        let ctx = ExecutionContext::new();
        assert_eq!(ctx.get_transaction_descriptor(), 0);
        assert_eq!(ctx.get_outstanding_requests(), 1);
        assert!(!ctx.has_open_batch());
        assert!(!ctx.has_open_result_set());
    }

    #[test]
    fn test_set_has_open_batch() {
        let mut ctx = ExecutionContext::new();
        assert!(!ctx.has_open_batch());
        ctx.set_has_open_batch(true);
        assert!(ctx.has_open_batch());
        ctx.set_has_open_batch(false);
        assert!(!ctx.has_open_batch());
    }

    #[test]
    fn test_set_has_open_result_set() {
        let mut ctx = ExecutionContext::new();
        assert!(!ctx.has_open_result_set());
        ctx.set_has_open_result_set(true);
        assert!(ctx.has_open_result_set());
        ctx.set_has_open_result_set(false);
        assert!(!ctx.has_open_result_set());
    }

    #[test]
    fn test_capture_begin_transaction() {
        let mut ctx = ExecutionContext::new();
        let change_token = EnvChangeToken {
            sub_type: EnvChangeTokenSubType::BeginTransaction,
            change_type: EnvChangeContainer::from((0_u64, 12345_u64)),
        };
        ctx.capture_change_property(&change_token).unwrap();
        assert_eq!(ctx.get_transaction_descriptor(), 12345);
    }

    #[test]
    fn test_capture_commit_transaction() {
        let mut ctx = ExecutionContext::new();
        ctx.transaction_descriptor = 999;
        let change_token = EnvChangeToken {
            sub_type: EnvChangeTokenSubType::CommitTransaction,
            change_type: EnvChangeContainer::from((999_u64, 0_u64)),
        };
        ctx.capture_change_property(&change_token).unwrap();
        assert_eq!(ctx.get_transaction_descriptor(), 0);
    }

    #[test]
    fn test_capture_rollback_transaction() {
        let mut ctx = ExecutionContext::new();
        ctx.transaction_descriptor = 888;
        let change_token = EnvChangeToken {
            sub_type: EnvChangeTokenSubType::RollbackTransaction,
            change_type: EnvChangeContainer::from((888_u64, 0_u64)),
        };
        ctx.capture_change_property(&change_token).unwrap();
        assert_eq!(ctx.get_transaction_descriptor(), 0);
    }

    #[test]
    fn test_capture_database_change() {
        let mut ctx = ExecutionContext::new();
        let change_token = EnvChangeToken {
            sub_type: EnvChangeTokenSubType::Database,
            change_type: EnvChangeContainer::from(("OldDB".to_string(), "NewDB".to_string())),
        };
        ctx.capture_change_property(&change_token).unwrap();
        assert_eq!(ctx.change_properties.database, Some("NewDB".to_string()));
    }

    #[test]
    fn test_capture_language_change() {
        let mut ctx = ExecutionContext::new();
        let change_token = EnvChangeToken {
            sub_type: EnvChangeTokenSubType::Language,
            change_type: EnvChangeContainer::from(("".to_string(), "us_english".to_string())),
        };
        ctx.capture_change_property(&change_token).unwrap();
        assert_eq!(
            ctx.change_properties.language,
            Some("us_english".to_string())
        );
    }

    #[test]
    fn test_capture_sql_collation() {
        let mut ctx = ExecutionContext::new();
        let collation = SqlCollation {
            info: 0,
            lcid_language_id: 1033,
            col_flags: 0,
            sort_id: 52,
        };
        let change_token = EnvChangeToken {
            sub_type: EnvChangeTokenSubType::SqlCollation,
            change_type: EnvChangeContainer::from((Some(SqlCollation::default()), Some(collation))),
        };
        ctx.capture_change_property(&change_token).unwrap();
        assert_eq!(
            ctx.change_properties
                .database_collation
                .unwrap()
                .lcid_language_id,
            1033
        );
    }

    #[test]
    fn test_capture_enlist_dtc_transaction() {
        let mut ctx = ExecutionContext::new();
        let change_token = EnvChangeToken {
            sub_type: EnvChangeTokenSubType::EnlistDtcTransaction,
            change_type: EnvChangeContainer::from((0_u64, 54321_u64)),
        };
        ctx.capture_change_property(&change_token).unwrap();
        assert_eq!(ctx.get_transaction_descriptor(), 54321);
    }

    #[test]
    fn test_capture_defect_transaction() {
        let mut ctx = ExecutionContext::new();
        ctx.transaction_descriptor = 777;
        let change_token = EnvChangeToken {
            sub_type: EnvChangeTokenSubType::DefectTransaction,
            change_type: EnvChangeContainer::from((777_u64, 0_u64)),
        };
        ctx.capture_change_property(&change_token).unwrap();
        assert_eq!(ctx.get_transaction_descriptor(), 0);
    }

    #[test]
    fn test_capture_unknown_subtype() {
        let mut ctx = ExecutionContext::new();
        let change_token = EnvChangeToken {
            sub_type: EnvChangeTokenSubType::Unknown(255),
            change_type: EnvChangeContainer::from((0_u64, 0_u64)),
        };
        // Should not error on unknown subtype
        assert!(ctx.capture_change_property(&change_token).is_ok());
    }

    #[test]
    fn test_capture_packet_size_error() {
        let mut ctx = ExecutionContext::new();
        let change_token = EnvChangeToken {
            sub_type: EnvChangeTokenSubType::PacketSize,
            change_type: EnvChangeContainer::from((0_u64, 0_u64)),
        };
        assert!(ctx.capture_change_property(&change_token).is_err());
    }

    #[test]
    fn test_already_executing_error_constant() {
        assert!(ALREADY_EXECUTING_ERROR.contains("open batch"));
    }
}
