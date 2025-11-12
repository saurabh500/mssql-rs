// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::io::Error;

use async_trait::async_trait;
use byteorder::{ByteOrder, LittleEndian};
use tracing::event;

use super::super::tokens::{EnvChangeToken, Tokens};
use super::common::TokenParser;
use crate::{core::TdsResult, io::packet_reader::TdsPacketReader};
use crate::{
    io::token_stream::ParserContext,
    message::login::RoutingInfo,
    token::tokens::{EnvChangeContainer, EnvChangeTokenSubType, SqlCollation, TokenType},
};

#[derive(Default)]
pub(crate) struct EnvChangeTokenParser {
    // fields omitted
}

#[derive(Debug, Default)]
#[cfg(fuzzing)]
pub struct EnvChangeTokenParser {
    // fields omitted
}

#[async_trait]
impl<T> TokenParser<T> for EnvChangeTokenParser
where
    T: TdsPacketReader + Send + Sync,
{
    async fn parse(&self, reader: &mut T, _context: &ParserContext) -> TdsResult<Tokens> {
        let _token_length = reader.read_uint16().await?;
        let sub_type = reader.read_byte().await?;
        let token_sub_type: EnvChangeTokenSubType = sub_type.try_into()?;
        event!(
            tracing::Level::DEBUG,
            "Parsing {:?} token with type and subtype {:?}",
            TokenType::EnvChange,
            token_sub_type
        );

        let token_value_change: EnvChangeContainer = match token_sub_type {
            EnvChangeTokenSubType::Database => {
                let new_value = reader.read_varchar_u8_length().await?;
                let old_value = reader.read_varchar_u8_length().await?;
                EnvChangeContainer::from((old_value, new_value))
            }
            EnvChangeTokenSubType::Language => {
                let new_value = reader.read_varchar_u8_length().await?;
                let old_value = reader.read_varchar_u8_length().await?;
                EnvChangeContainer::from((old_value, new_value))
            }
            EnvChangeTokenSubType::CharacterSet => {
                let new_value = reader.read_varchar_u8_length().await?;
                let old_value = reader.read_varchar_u8_length().await?;
                EnvChangeContainer::from((old_value, new_value))
            }
            EnvChangeTokenSubType::PacketSize => {
                let new_value_string = reader.read_varchar_u8_length().await?;
                let old_value_string = reader.read_varchar_u8_length().await?;
                let new_value = new_value_string.parse::<u32>().map_err(|_| {
                    Error::new(std::io::ErrorKind::InvalidData, "Invalid new packet size")
                })?;
                let old_value = old_value_string.parse::<u32>().map_err(|_| {
                    Error::new(std::io::ErrorKind::InvalidData, "Invalid old packet size")
                })?;
                EnvChangeContainer::from((old_value, new_value))
            }
            EnvChangeTokenSubType::UnicodeDataSortingLocalId => {
                return Err(crate::error::Error::UnimplementedFeature {
                    feature: "UnicodeDataSortingLocalId".to_string(),
                    context: "EnvChange token parsing not yet implemented".to_string(),
                });
            }
            EnvChangeTokenSubType::UnicodeDataSortingComparisonFlags => {
                return Err(crate::error::Error::UnimplementedFeature {
                    feature: "UnicodeDataSortingComparisonFlags".to_string(),
                    context: "EnvChange token parsing not yet implemented".to_string(),
                });
            }
            EnvChangeTokenSubType::SqlCollation => {
                let new_bytes = reader.read_u8_varbyte().await?;
                let old_bytes = reader.read_u8_varbyte().await?;
                let old_collation: Option<SqlCollation> = match old_bytes.len() {
                    5 => old_bytes.as_slice().try_into().ok(),
                    _ => None,
                };

                let new_collation: Option<SqlCollation> = match new_bytes.len() {
                    5 => new_bytes.as_slice().try_into().ok(),
                    _ => None,
                };
                EnvChangeContainer::from((old_collation, new_collation))
            }
            EnvChangeTokenSubType::BeginTransaction
            | EnvChangeTokenSubType::EnlistDtcTransaction => {
                let new_value = reader.read_u8_varbyte().await?;
                let new_descriptor = match new_value.len() {
                    8 => Ok(LittleEndian::read_u64(&new_value)),
                    _ => Err(Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid new transaction descriptor",
                    )),
                }?;
                let old_value = reader.read_u8_varbyte().await?;
                let old_descriptor = match old_value.len() {
                    0 => Ok(0u64),
                    _ => Err(Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid old transaction descriptor",
                    )),
                }?;
                EnvChangeContainer::from((old_descriptor, new_descriptor))
            }
            EnvChangeTokenSubType::CommitTransaction => {
                let new_value = reader.read_u8_varbyte().await?;
                let new_descriptor: u64 = match new_value.len() {
                    0 => Ok(0u64),
                    _ => Err(Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid new transaction descriptor",
                    )),
                }?;
                let old_value = reader.read_u8_varbyte().await?;
                let old_descriptor = match old_value.len() {
                    8 => Ok(LittleEndian::read_u64(&old_value)),
                    _ => Err(Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid old transaction descriptor",
                    )),
                }?;
                EnvChangeContainer::from((old_descriptor, new_descriptor))
            }
            EnvChangeTokenSubType::RollbackTransaction => {
                let new_value = reader.read_u8_varbyte().await?;
                let new_descriptor: u64 = match new_value.len() {
                    0 => Ok(0u64),
                    _ => Err(Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid new transaction descriptor",
                    )),
                }?;
                let old_value = reader.read_u8_varbyte().await?;
                let old_descriptor = match old_value.len() {
                    8 => Ok(LittleEndian::read_u64(&old_value)),
                    _ => Err(Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid old transaction descriptor",
                    )),
                }?;
                EnvChangeContainer::from((old_descriptor, new_descriptor))
            }
            EnvChangeTokenSubType::DefectTransaction => {
                let new_value = reader.read_u8_varbyte().await?;
                let new_descriptor = match new_value.len() {
                    8 => Ok(LittleEndian::read_u64(&new_value)),
                    _ => Err(Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid new transaction descriptor",
                    )),
                }?;
                let old_value = reader.read_u8_varbyte().await?;
                let old_descriptor = match old_value.len() {
                    0 => Ok(0u64),
                    _ => Err(crate::error::Error::ProtocolError(
                        "Invalid old transaction descriptor".to_string(),
                    )),
                }?;
                EnvChangeContainer::from((old_descriptor, new_descriptor))
            }
            EnvChangeTokenSubType::DatabaseMirroringPartner => {
                return Err(crate::error::Error::UnimplementedFeature {
                    feature: "DatabaseMirroringPartner".to_string(),
                    context: "EnvChange token parsing not yet implemented".to_string(),
                });
            }
            EnvChangeTokenSubType::PromoteTransaction => {
                return Err(crate::error::Error::UnimplementedFeature {
                    feature: "PromoteTransaction".to_string(),
                    context: "EnvChange token parsing not yet implemented".to_string(),
                });
            }
            EnvChangeTokenSubType::TransactionManagerAddress => {
                return Err(crate::error::Error::UnimplementedFeature {
                    feature: "TransactionManagerAddress".to_string(),
                    context: "EnvChange token parsing not yet implemented".to_string(),
                });
            }
            EnvChangeTokenSubType::TransactionEnded => {
                return Err(crate::error::Error::UnimplementedFeature {
                    feature: "TransactionEnded".to_string(),
                    context: "EnvChange token parsing not yet implemented".to_string(),
                });
            }
            EnvChangeTokenSubType::ResetConnection => {
                let new_value = reader.read_u8_varbyte().await?;
                let old_value = reader.read_u8_varbyte().await?;
                EnvChangeContainer::from((old_value, new_value))
            }
            EnvChangeTokenSubType::UserInstanceName => {
                return Err(crate::error::Error::UnimplementedFeature {
                    feature: "UserInstanceName".to_string(),
                    context: "EnvChange token parsing not yet implemented".to_string(),
                });
            }
            EnvChangeTokenSubType::Routing => {
                let _length = reader.read_uint16().await?;
                let protocol = reader.read_byte().await?;
                let port = reader.read_uint16().await?;
                let server = reader.read_varchar_u16_length().await?;
                let routing_info = Some(RoutingInfo {
                    protocol,
                    port,
                    server: server.unwrap_or_default(),
                });

                let mut old_routing_info: Option<RoutingInfo> = None;

                let old_length = reader.read_uint16().await?;
                if old_length > 0 {
                    let old_protocol = reader.read_byte().await?;
                    let old_port = reader.read_uint16().await?;
                    let old_server = reader.read_varchar_u16_length().await?;

                    old_routing_info = Some(RoutingInfo {
                        protocol: old_protocol,
                        port: old_port,
                        server: old_server.unwrap_or_default(),
                    });
                }
                EnvChangeContainer::from((old_routing_info, routing_info))
            }
            EnvChangeTokenSubType::Unknown(_value) => {
                // For unknown environment change subtypes, try to read as generic string change
                let new_value = reader.read_varchar_u8_length().await?;
                let old_value = reader.read_varchar_u8_length().await?;
                EnvChangeContainer::from((old_value, new_value))
            }
        };
        Ok(Tokens::from(EnvChangeToken {
            sub_type: token_sub_type,
            change_type: token_value_change,
        }))
    }
}
