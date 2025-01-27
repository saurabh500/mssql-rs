use std::io::Error;

use async_trait::async_trait;
use tracing::event;

use crate::{
    core::Version,
    message::{login::RoutingInfo, login_options::TdsVersion},
    read_write::packet_reader::PacketReader,
    token::{
        fed_auth_info::FedAuthInfoId,
        login_ack::{LoginAck, SqlInterfaceType},
        tokens::{
            CharsetEnvChangeToken, CurrentCommand, DatabaseEnvChangeToken, DoneStatus,
            EnvChangeTokenSubType, InfoToken, LanguageEnvChangeToken, PacketSizeEnvChangeToken,
            RoutingEnvChangeToken, SqlCollation, SqlCollationEnvChangeToken, TokenType,
        },
    },
};

use super::{
    fed_auth_info::FedAuthInfoToken,
    tokens::{DoneInProcToken, DoneProcToken, DoneToken, ErrorToken, Token},
};

#[async_trait]
pub trait TokenParser {
    async fn parse(&self, packet_reader: &mut PacketReader) -> Result<Box<dyn Token>, Error>;
}

pub(crate) struct EnvChangeTokenParser {
    // fields omitted
}

#[async_trait]
impl TokenParser for EnvChangeTokenParser {
    async fn parse(&self, reader: &mut PacketReader) -> Result<Box<dyn Token>, Error> {
        let _token_length = reader.read_uint16().await?;
        let sub_type = reader.read_byte().await?;
        let token_sub_type = EnvChangeTokenSubType::from(sub_type);
        event!(
            tracing::Level::DEBUG,
            "Parsing {:?} token with type and subtype {:?}",
            TokenType::EnvChange,
            token_sub_type
        );
        event!(
            tracing::Level::DEBUG,
            "Parsing EnvChangeSubtype {:?} token",
            token_sub_type
        );

        let token: Box<dyn Token> = match token_sub_type {
            EnvChangeTokenSubType::Database => {
                let new_value = reader.read_varchar_u8_length().await?;
                let old_value = reader.read_varchar_u8_length().await?;
                Box::new(DatabaseEnvChangeToken::new(old_value, new_value))
            }
            EnvChangeTokenSubType::Language => {
                let new_value = reader.read_varchar_u8_length().await?;
                let old_value = reader.read_varchar_u8_length().await?;
                Box::new(LanguageEnvChangeToken::new(old_value, new_value))
            }
            EnvChangeTokenSubType::CharacterSet => {
                let new_value = reader.read_varchar_u8_length().await?;
                let old_value = reader.read_varchar_u8_length().await?;
                Box::new(CharsetEnvChangeToken::new(old_value, new_value))
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
                Box::new(PacketSizeEnvChangeToken::new(old_value, new_value))
            }
            EnvChangeTokenSubType::UnicodeDataSortingLocalId => todo!(),
            EnvChangeTokenSubType::UnicodeDataSortingComparisonFlags => todo!(),
            EnvChangeTokenSubType::SqlCollation => {
                let old_bytes = reader.read_u8_varbyte().await?;
                let new_bytes = reader.read_u8_varbyte().await?;
                let old_collation = SqlCollation::new(&old_bytes);
                let new_collation = SqlCollation::new(&new_bytes);
                Box::new(SqlCollationEnvChangeToken::new(
                    old_collation,
                    new_collation,
                ))
            }
            EnvChangeTokenSubType::BeginTransaction => todo!(),
            EnvChangeTokenSubType::CommitTransaction => todo!(),
            EnvChangeTokenSubType::RollbackTransaction => todo!(),
            EnvChangeTokenSubType::EnlistDtcTransaction => todo!(),
            EnvChangeTokenSubType::DefectTransaction => todo!(),
            EnvChangeTokenSubType::DatabaseMirroringPartner => todo!(),
            EnvChangeTokenSubType::PromoteTransaction => todo!(),
            EnvChangeTokenSubType::TransactionManagerAddress => todo!(),
            EnvChangeTokenSubType::TransactionEnded => todo!(),
            EnvChangeTokenSubType::ResetConnection => todo!(),
            EnvChangeTokenSubType::UserInstanceName => todo!(),
            EnvChangeTokenSubType::Routing => {
                let _length = reader.read_uint16().await?;
                let protocol = reader.read_byte().await?;
                let port = reader.read_uint16().await?;
                let server = reader.read_varchar_u16_length().await?;
                let routing_info = Some(RoutingInfo {
                    protocol,
                    port,
                    server: server.unwrap(),
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
                        server: old_server.unwrap(),
                    });
                }

                Box::new(RoutingEnvChangeToken::new(routing_info, old_routing_info))
            }
        };

        Ok(token)
    }
}

pub(crate) struct LoginAckTokenParser {
    // fields omitted
}

#[async_trait]
impl TokenParser for LoginAckTokenParser {
    async fn parse(&self, reader: &mut PacketReader) -> Result<Box<dyn Token>, Error> {
        event!(
            tracing::Level::DEBUG,
            "Parsing LoginAck token with type: 0x{:02X}",
            TokenType::LoginAck as u8
        );
        let _length = reader.read_uint16().await?;
        let interface_type = reader.read_byte().await?;
        let interface = SqlInterfaceType::from(interface_type);

        let tds_version = reader.read_int32_big_endian().await?;

        let tds_version = TdsVersion::from(tds_version);

        let prog_name = reader.read_varchar_u8_length().await?;
        let major = reader.read_byte().await?;
        let minor = reader.read_byte().await?;
        let build_hi = reader.read_byte().await?;
        let build_low = reader.read_byte().await?;

        let prog_version = Version::new(
            major,
            minor,
            (((build_hi as u16) << 8) | build_low as u16) as u16,
            0,
        );
        Ok(Box::new(LoginAck {
            interface_type: interface,
            tds_version,
            prog_name,
            prog_version,
        }))
    }
}

pub(crate) struct DoneTokenParser {
    // fields omitted
}

#[async_trait]
impl TokenParser for DoneTokenParser {
    async fn parse(&self, reader: &mut PacketReader) -> Result<Box<dyn Token>, Error> {
        let status = reader.read_uint16().await?;
        let done_status = DoneStatus::from(status);
        let current_command_value = reader.read_uint16().await?;
        let current_command = CurrentCommand::try_from(current_command_value).unwrap();
        let row_count = reader.read_uint64().await?;

        Ok(Box::new(DoneToken {
            status: done_status,
            cur_cmd: current_command,
            row_count,
        }))
    }
}

pub(crate) struct DoneInProcTokenParser {
    // fields omitted
}

#[async_trait]
impl TokenParser for DoneInProcTokenParser {
    async fn parse(&self, reader: &mut PacketReader) -> Result<Box<dyn Token>, Error> {
        let status = reader.read_uint16().await?;
        let done_status = DoneStatus::from(status);
        let current_command_value = reader.read_uint16().await?;
        let current_command = CurrentCommand::try_from(current_command_value).unwrap();
        let row_count = reader.read_uint64().await?;

        Ok(Box::new(DoneInProcToken {
            status: done_status,
            cur_cmd: current_command,
            row_count,
        }))
    }
}

pub(crate) struct DoneProcTokenParser {
    // fields omitted
}

#[async_trait]
impl TokenParser for DoneProcTokenParser {
    async fn parse(&self, reader: &mut PacketReader) -> Result<Box<dyn Token>, Error> {
        let status = reader.read_uint16().await?;
        let done_status = DoneStatus::from(status);
        let current_command_value = reader.read_uint16().await?;
        let current_command = CurrentCommand::try_from(current_command_value).unwrap();
        let row_count = reader.read_uint64().await?;

        Ok(Box::new(DoneProcToken {
            status: done_status,
            cur_cmd: current_command,
            row_count,
        }))
    }
}

pub(crate) struct InfoTokenParser {
    // fields omitted
}

#[async_trait]
impl TokenParser for InfoTokenParser {
    async fn parse(&self, reader: &mut PacketReader) -> Result<Box<dyn Token>, Error> {
        let _length = reader.read_uint16().await?;
        let number = reader.read_uint32().await?;
        let state = reader.read_byte().await?;
        let severity = reader.read_byte().await?;
        let message = reader.read_varchar_u16_length().await?;
        let server_name = reader.read_varchar_u8_length().await?;
        let proc_name = reader.read_varchar_u8_length().await?;
        let line_number = reader.read_uint32().await?;

        event!(tracing::Level::INFO, "Info message: {:?}", message);

        Ok(Box::new(InfoToken {
            number,
            state,
            severity,
            message: message.unwrap(),
            server_name,
            proc_name,
            line_number,
        }))
    }
}

pub(crate) struct ErrorTokenParser {
    // fields omitted
}

#[async_trait]
impl TokenParser for ErrorTokenParser {
    async fn parse(&self, reader: &mut PacketReader) -> Result<Box<dyn Token>, Error> {
        event!(
            tracing::Level::DEBUG,
            "Parsing Error token with type: 0x{:02X}",
            TokenType::Error as u8
        );
        let _ = reader.read_uint16().await?;
        let number = reader.read_uint32().await?;
        let state = reader.read_byte().await?;
        let severity = reader.read_byte().await?;

        let message = reader.read_varchar_u16_length().await?.unwrap();
        event!(tracing::Level::ERROR, "Error message: {:?}", message);
        let server_name = reader.read_varchar_u8_length().await?;
        let proc_name = reader.read_varchar_u8_length().await?;

        let line_number = reader.read_uint32().await?;

        Ok(Box::new(ErrorToken {
            number,
            state,
            severity,
            message,
            server_name,
            proc_name,
            line_number,
        }))
    }
}

pub(crate) struct FedAuthInfoTokenParser {
    // fields omitted
}

impl FedAuthInfoTokenParser {
    const FEDAUTH_OPTIONS_SIZE: u32 = 9;
}

#[async_trait]
impl TokenParser for FedAuthInfoTokenParser {
    async fn parse(&self, reader: &mut PacketReader) -> Result<Box<dyn Token>, Error> {
        let _length = reader.read_int32().await?;

        let options_count = reader.read_uint32().await?;
        let data_left = _length - size_of::<u32>() as i32;

        let mut token_data: Vec<u8> = vec![0; data_left as usize];
        reader.read_bytes(&mut token_data[0..]).await?;

        let mut sts_url = String::new();
        let mut spn = String::new();
        for i in 0..options_count {
            let current_options_offset = i * Self::FEDAUTH_OPTIONS_SIZE;
            let option_id = token_data[current_options_offset as usize];
            let option_data_length = u32::from_le_bytes(
                token_data
                    [(current_options_offset + 1) as usize..(current_options_offset + 5) as usize]
                    .try_into()
                    .unwrap(),
            );
            let mut option_data_offset = u32::from_le_bytes(
                token_data
                    [(current_options_offset + 5) as usize..(current_options_offset + 9) as usize]
                    .try_into()
                    .unwrap(),
            );

            option_data_offset -= size_of::<u32>() as u32;
            let string_bytes: &[u8] = token_data
                [option_data_offset as usize..(option_data_offset + option_data_length) as usize]
                .try_into()
                .unwrap();
            let value = String::from_utf8(string_bytes.to_vec()).map_err(|_| {
                Error::new(std::io::ErrorKind::InvalidData, "Invalid UTF-8 sequence")
            })?;

            event!(
                tracing::Level::DEBUG,
                "FedAuth option: {:?} with value: {:?}",
                option_id,
                value.clone()
            );

            match Into::<FedAuthInfoId>::into(option_id) {
                FedAuthInfoId::STSUrl => {
                    sts_url = value;
                }
                FedAuthInfoId::SPN => {
                    spn = value;
                }
            }
        }

        Ok(Box::new(FedAuthInfoToken { spn, sts_url }))
    }
}

pub(crate) struct FeatureExtAckTokenParser {
    // fields omitted
}

#[async_trait]
impl TokenParser for FeatureExtAckTokenParser {
    async fn parse(&self, _reader: &mut PacketReader) -> Result<Box<dyn Token>, Error> {
        unimplemented!()
    }
}
