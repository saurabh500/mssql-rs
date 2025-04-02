use std::{io::Error, vec};

use async_trait::async_trait;
use tracing::{debug, error, event, trace};

use super::{
    fed_auth_info::FedAuthInfoToken,
    tokens::{
        DoneInProcToken, DoneProcToken, DoneToken, EnvChangeToken, ErrorToken, FeatureExtAckToken,
        ReturnStatusToken, RowToken, Tokens,
    },
};
use crate::core::TdsResult;
use crate::{
    core::Version,
    datatypes::{
        decoder::{ColumnValues, SqlTypeDecode},
        sqldatatypes::{read_type_info, TdsDataType},
    },
    message::{
        login::{FeatureExtension, RoutingInfo},
        login_options::TdsVersion,
    },
    query::metadata::{ColumnMetadata, MultiPartName},
    read_write::{packet_reader::PacketReader, token_stream::ParserContext},
    token::{
        fed_auth_info::FedAuthInfoId,
        login_ack::{LoginAckToken, SqlInterfaceType},
        tokens::{
            ColMetadataToken, CurrentCommand, DoneStatus, EnvChangeContainer,
            EnvChangeTokenSubType, InfoToken, OrderToken, SqlCollation, TokenType,
        },
    },
};

#[async_trait]
pub(crate) trait TokenParser<'a> {
    async fn parse(
        &self,
        reader: &'a mut PacketReader,
        context: &ParserContext,
    ) -> TdsResult<Tokens>;
}

#[derive(Debug, Default)]
pub(crate) struct EnvChangeTokenParser {
    // fields omitted
}

#[async_trait]
impl<'a> TokenParser<'a> for EnvChangeTokenParser {
    async fn parse(
        &self,
        reader: &'a mut PacketReader,
        _context: &ParserContext,
    ) -> TdsResult<Tokens> {
        let _token_length = reader.read_uint16().await?;
        let sub_type = reader.read_byte().await?;
        let token_sub_type = EnvChangeTokenSubType::from(sub_type);
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
            EnvChangeTokenSubType::UnicodeDataSortingLocalId => todo!(),
            EnvChangeTokenSubType::UnicodeDataSortingComparisonFlags => todo!(),
            EnvChangeTokenSubType::SqlCollation => {
                let old_bytes = reader.read_u8_varbyte().await?;
                let new_bytes = reader.read_u8_varbyte().await?;
                let old_collation = match old_bytes.len() {
                    5 => Some(SqlCollation::new(&old_bytes)),
                    _ => None,
                };

                let new_collation = match new_bytes.len() {
                    5 => Some(SqlCollation::new(&old_bytes)),
                    _ => None,
                };
                EnvChangeContainer::from((old_collation, new_collation))
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
                EnvChangeContainer::from((old_routing_info, routing_info))
            }
        };
        Ok(Tokens::from(EnvChangeToken {
            sub_type: token_sub_type,
            change_type: token_value_change,
        }))
    }
}

#[derive(Debug, Default)]
pub(crate) struct LoginAckTokenParser {
    // fields omitted
}

#[async_trait]
impl<'a> TokenParser<'a> for LoginAckTokenParser {
    async fn parse(
        &self,
        reader: &'a mut PacketReader,
        _context: &ParserContext,
    ) -> TdsResult<Tokens> {
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

        let prog_version =
            Version::new(major, minor, ((build_hi as u16) << 8) | build_low as u16, 0);
        Ok(Tokens::from(LoginAckToken {
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
impl<'a> TokenParser<'a> for DoneTokenParser {
    async fn parse(
        &self,
        reader: &'a mut PacketReader,
        _context: &ParserContext,
    ) -> TdsResult<Tokens> {
        let status = reader.read_uint16().await?;
        let done_status = DoneStatus::from(status);
        let current_command_value = reader.read_uint16().await?;
        let current_command = CurrentCommand::try_from(current_command_value).unwrap();
        let row_count = reader.read_uint64().await?;

        Ok(Tokens::from(DoneToken {
            status: done_status,
            cur_cmd: current_command,
            row_count,
        }))
    }
}

#[derive(Debug, Default)]
pub(crate) struct DoneInProcTokenParser {
    // fields omitted
}

#[async_trait]
impl<'a> TokenParser<'a> for DoneInProcTokenParser {
    async fn parse(
        &self,
        reader: &'a mut PacketReader,
        _context: &ParserContext,
    ) -> TdsResult<Tokens> {
        let status = reader.read_uint16().await?;
        let done_status = DoneStatus::from(status);
        let current_command_value = reader.read_uint16().await?;
        let current_command = CurrentCommand::try_from(current_command_value).unwrap();
        let row_count = reader.read_uint64().await?;

        Ok(Tokens::from(DoneInProcToken {
            status: done_status,
            cur_cmd: current_command,
            row_count,
        }))
    }
}

#[derive(Debug, Default)]
pub(crate) struct DoneProcTokenParser {
    // fields omitted
}

#[async_trait]
impl<'a> TokenParser<'a> for DoneProcTokenParser {
    async fn parse(
        &self,
        reader: &'a mut PacketReader,
        _context: &ParserContext,
    ) -> TdsResult<Tokens> {
        let status = reader.read_uint16().await?;
        let done_status = DoneStatus::from(status);
        let current_command_value = reader.read_uint16().await?;
        let current_command = CurrentCommand::try_from(current_command_value).unwrap();
        let row_count = reader.read_uint64().await?;

        Ok(Tokens::from(DoneProcToken {
            status: done_status,
            cur_cmd: current_command,
            row_count,
        }))
    }
}

#[derive(Debug, Default)]
pub(crate) struct InfoTokenParser {
    // fields omitted
}

#[async_trait]
impl<'a> TokenParser<'a> for InfoTokenParser {
    async fn parse(
        &self,
        reader: &'a mut PacketReader,
        _context: &ParserContext,
    ) -> TdsResult<Tokens> {
        let _length = reader.read_uint16().await?;
        let number = reader.read_uint32().await?;
        let state = reader.read_byte().await?;
        let severity = reader.read_byte().await?;
        let message = reader.read_varchar_u16_length().await?;
        let server_name = reader.read_varchar_u8_length().await?;
        let proc_name = reader.read_varchar_u8_length().await?;
        let line_number = reader.read_uint32().await?;

        event!(tracing::Level::INFO, "Info message: {:?}", message);

        Ok(Tokens::from(InfoToken {
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

#[derive(Debug, Default)]
pub(crate) struct ErrorTokenParser {
    // fields omitted
}

#[async_trait]
impl<'a> TokenParser<'a> for ErrorTokenParser {
    async fn parse(
        &self,
        reader: &'a mut PacketReader,
        _context: &ParserContext,
    ) -> TdsResult<Tokens> {
        error!(
            "Parsing Error token with type: 0x{:02X}",
            TokenType::Error as u8
        );
        let _ = reader.read_uint16().await?;
        let number = reader.read_uint32().await?;
        let state = reader.read_byte().await?;
        let severity = reader.read_byte().await?;

        let message = reader.read_varchar_u16_length().await?.unwrap();
        error!("Error message: {:?}", message);
        let server_name = reader.read_varchar_u8_length().await?;
        let proc_name = reader.read_varchar_u8_length().await?;

        let line_number = reader.read_uint32().await?;

        Ok(Tokens::from(ErrorToken {
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
#[derive(Debug, Default)]
pub(crate) struct FedAuthInfoTokenParser {
    // fields omitted
}

impl FedAuthInfoTokenParser {
    const FEDAUTH_OPTIONS_SIZE: u32 = 9;
}

#[async_trait]
impl<'a> TokenParser<'a> for FedAuthInfoTokenParser {
    async fn parse(
        &self,
        reader: &'a mut PacketReader,
        _context: &ParserContext,
    ) -> TdsResult<Tokens> {
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

            debug!(
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

        Ok(Tokens::from(FedAuthInfoToken { spn, sts_url }))
    }
}
#[derive(Debug, Default)]
pub(crate) struct FeatureExtAckTokenParser {
    // fields omitted
}

#[async_trait]
impl<'a> TokenParser<'a> for FeatureExtAckTokenParser {
    async fn parse(
        &self,
        reader: &'a mut PacketReader,
        _context: &ParserContext,
    ) -> TdsResult<Tokens> {
        let mut features: Vec<(FeatureExtension, Vec<u8>)> = Vec::new();
        loop {
            let feature_identifier = FeatureExtension::from(reader.read_byte().await?);
            if feature_identifier == FeatureExtension::Terminator {
                break;
            }
            let data_length = reader.read_uint32().await?;
            let mut feature_data_buffer = vec![0; data_length as usize];

            if data_length > 0 {
                reader.read_bytes(&mut feature_data_buffer[0..]).await?;
                // Store the features somewhere.
            }
            features.push((feature_identifier, feature_data_buffer));
        }
        Ok(Tokens::from(FeatureExtAckToken::new(features)))
    }
}

#[derive(Debug, Default)]
pub(crate) struct ColMetadataTokenParser {
    // Do we want to create a new parser for every connection, or should
    // this value be passed as a context to the parser? Likely SessionSettings?
    pub is_column_encryption_supported: bool,
}

impl ColMetadataTokenParser {
    pub fn new(is_column_encryption_supported: bool) -> Self {
        Self {
            is_column_encryption_supported,
        }
    }

    pub fn is_column_encryption_supported(&self) -> bool {
        self.is_column_encryption_supported
    }
}

#[async_trait]
impl<'a> TokenParser<'a> for ColMetadataTokenParser {
    async fn parse(
        &self,
        packet_reader: &'a mut PacketReader,
        _context: &ParserContext,
    ) -> TdsResult<Tokens> {
        // Allocate a heap pointer so that we can reference the reader
        // by passing it around into other methods.
        let mut reader = Box::new(packet_reader);

        let col_count = reader.read_uint16().await?;

        if self.is_column_encryption_supported {
            unimplemented!("Column encryption is not yet supported");
        }

        // Handle the special case where no metadata is sent
        if col_count == 0xFFFF {
            return Ok(Tokens::from(ColMetadataToken::default()));
        }

        let mut column_metadata: Vec<ColumnMetadata> = Vec::with_capacity(col_count as usize);
        for _ in 0..col_count {
            let user_type = reader.read_uint32().await?;

            let flags = reader.read_uint16().await?;

            let raw_data_type = reader.read_byte().await?;
            let some_data_type = TdsDataType::try_from(raw_data_type);
            if some_data_type.is_err() {
                return Err(crate::error::Error::from(Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Invalid data type: {}", raw_data_type),
                )));
            }
            let data_type = some_data_type?;
            let type_info = read_type_info(&mut reader, data_type).await?;

            // Parse Table name
            // TDS Doc snippet
            // The fully qualified base table name for this column.
            // It contains the table name length and table name.
            // This exists only for text, ntext, and image columns. It specifies the number of parts that are returned and then repeats PartName once for each NumParts.
            let multi_part_name = match data_type {
                TdsDataType::Text | TdsDataType::NText | TdsDataType::Image => {
                    let mut part_count = reader.read_byte().await?;
                    if part_count == 0 {
                        None
                    } else {
                        let mut mpt = MultiPartName::default();
                        while part_count > 0 {
                            let part_name = reader.read_varchar_u16_length().await?;
                            if part_count == 4 {
                                mpt.server_name = part_name;
                            } else if part_count == 3 {
                                mpt.catalog_name = part_name;
                            } else if part_count == 2 {
                                mpt.schema_name = part_name;
                            } else if part_count == 1 {
                                mpt.table_name = part_name.unwrap();
                            }
                            part_count -= 1;
                        }
                        Some(mpt)
                    }
                }
                _ => None,
            };

            let col_name = reader.read_varchar_u8_length().await?;

            let col_metadata = ColumnMetadata {
                user_type,
                flags,
                data_type,
                type_info,
                column_name: col_name,
                multi_part_name,
            };
            if col_metadata.is_encrypted() {
                unimplemented!("Column encryption is not yet supported");
            }

            column_metadata.push(col_metadata);
        }
        let metadata = ColMetadataToken {
            column_count: col_count,
            columns: column_metadata,
        };
        Ok(Tokens::from(metadata))
    }
}

#[derive(Debug)]
pub(crate) struct RowTokenParser<T>
where
    T: for<'a> SqlTypeDecode<'a>,
{
    // fields omitted
    decoder: T,
}

impl<T: for<'a> SqlTypeDecode<'a> + Default> Default for RowTokenParser<T> {
    fn default() -> Self {
        Self {
            decoder: T::default(),
        }
    }
}

#[async_trait]
impl<'a, T: for<'b> SqlTypeDecode<'b> + Sync> TokenParser<'a> for RowTokenParser<T> {
    async fn parse(
        &self,
        reader: &'a mut PacketReader,
        context: &ParserContext,
    ) -> TdsResult<Tokens> {
        let column_metadata_token = match context {
            ParserContext::ColumnMetadata(metadata) => {
                trace!("Metadata during Row Parsing: {:?}", metadata);
                metadata
            }
            _ => {
                debug_assert!(false, "Expected ColumnMetadata in context");
                return Err(crate::error::Error::from(Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Expected ColumnMetadata in context",
                )));
            }
        };

        let all_metadata = &column_metadata_token.columns;
        let mut all_values: Vec<ColumnValues> =
            Vec::with_capacity(column_metadata_token.column_count as usize);
        for metadata in all_metadata {
            trace!("Metadata: {:?}", metadata);
            let column_value = self.decoder.decode(reader, metadata).await?;

            all_values.push(column_value);
        }
        Ok(Tokens::from(RowToken::new(all_values)))
    }
}

#[derive(Debug, Default)]
pub(crate) struct OrderTokenParser {}

#[async_trait]
impl<'a> TokenParser<'a> for OrderTokenParser {
    async fn parse(
        &self,
        reader: &'a mut PacketReader,
        _context: &ParserContext,
    ) -> TdsResult<Tokens> {
        let length = reader.read_uint16().await?;

        let col_count = length / 2;
        let mut columns = vec![];
        for _ in 0..col_count {
            columns.push(reader.read_uint16().await?);
        }
        Ok(Tokens::from(OrderToken {
            order_columns: columns,
        }))
    }
}

#[derive(Debug, Default)]
pub(crate) struct ReturnStatusTokenParser {}

#[async_trait]
impl<'a> TokenParser<'a> for ReturnStatusTokenParser {
    async fn parse(
        &self,
        reader: &'a mut PacketReader,
        _context: &ParserContext,
    ) -> TdsResult<Tokens> {
        let value = reader.read_int32().await?;

        Ok(Tokens::from(ReturnStatusToken { value }))
    }
}

#[derive(Debug)]
pub(crate) struct NbcRowTokenParser<T>
where
    T: for<'a> SqlTypeDecode<'a>,
{
    // fields omitted
    decoder: T,
}

impl<T: for<'a> SqlTypeDecode<'a> + Default> Default for NbcRowTokenParser<T> {
    fn default() -> Self {
        Self {
            decoder: T::default(),
        }
    }
}

fn is_null_value_in_column(null_bitmap: &[u8], index: usize) -> bool {
    let byte_index: usize = index / 8;
    let bit_index = index % 8;
    (null_bitmap[byte_index] & (1 << bit_index)) != 0
}

#[async_trait]
impl<'a, T: for<'b> SqlTypeDecode<'b> + Sync> TokenParser<'a> for NbcRowTokenParser<T> {
    async fn parse(
        &self,
        reader: &'a mut PacketReader,
        context: &ParserContext,
    ) -> TdsResult<Tokens> {
        let column_metadata_token = match context {
            ParserContext::ColumnMetadata(metadata) => {
                trace!("Metadata during Row Parsing: {:?}", metadata);
                metadata
            }
            _ => {
                debug_assert!(false, "Expected ColumnMetadata in context");
                return Err(crate::error::Error::from(Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Expected ColumnMetadata in context",
                )));
            }
        };

        let all_metadata = &column_metadata_token.columns;
        let mut all_values: Vec<ColumnValues> =
            Vec::with_capacity(column_metadata_token.column_count as usize);
        let col_count = all_metadata.len();

        let bitmap_length = (col_count + 7) / 8;
        let mut bitmap: Vec<u8> = vec![0; bitmap_length as usize];
        reader.read_bytes(bitmap.as_mut_slice()).await?;
        // let mut index = 0;

        for (index, metadata) in all_metadata.iter().enumerate() {
            trace!("Metadata: {:?}", metadata);
            let is_null = is_null_value_in_column(&bitmap, index);

            if is_null {
                all_values.push(ColumnValues::Null);
            } else {
                let column_value = self.decoder.decode(reader, metadata).await?;
                all_values.push(column_value);
            }
        }
        Ok(Tokens::from(RowToken::new(all_values)))
    }
}
