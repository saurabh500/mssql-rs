// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{io::Error, vec};

use async_trait::async_trait;
use tracing::debug;

use super::common::TokenParser;
use super::super::fed_auth_info::FedAuthInfoToken;
use super::super::tokens::Tokens;
use crate::{core::TdsResult, io::packet_reader::TdsPacketReader};
use crate::{
    io::token_stream::ParserContext,
    token::fed_auth_info::FedAuthInfoId,
};

#[derive(Default)]
pub(crate) struct FedAuthInfoTokenParser {
    // fields omitted
}

impl FedAuthInfoTokenParser {
    const FEDAUTH_OPTIONS_SIZE: u32 = 9;
}

#[async_trait]
impl<T> TokenParser<T> for FedAuthInfoTokenParser
where
    T: TdsPacketReader + Send + Sync,
{
    async fn parse(&self, reader: &mut T, _context: &ParserContext) -> TdsResult<Tokens> {
        let length = reader.read_int32().await?;

        let options_count = reader.read_uint32().await?;
        let data_left = length - size_of::<u32>() as i32;

        // Validate data_left to prevent capacity overflow attacks
        const MAX_TOKEN_DATA_SIZE: i32 = 1024 * 1024; // 1MB reasonable limit
        if !(0..=MAX_TOKEN_DATA_SIZE).contains(&data_left) {
            return Err(crate::error::Error::ProtocolError(format!(
                "Invalid FedAuthInfo token data size: {data_left} bytes (length: {length}, options_count: {options_count}). Must be between 0 and {MAX_TOKEN_DATA_SIZE} bytes."
            )));
        }

        // Validate that we have enough data for the options_count
        // Each option requires FEDAUTH_OPTIONS_SIZE bytes
        let required_size = options_count
            .checked_mul(Self::FEDAUTH_OPTIONS_SIZE)
            .ok_or_else(|| {
                crate::error::Error::ProtocolError(format!(
                    "FedAuthInfo options_count overflow: {options_count} * {} would overflow",
                    Self::FEDAUTH_OPTIONS_SIZE
                ))
            })?;

        if required_size as i32 > data_left {
            return Err(crate::error::Error::ProtocolError(format!(
                "Invalid FedAuthInfo token: options_count ({options_count}) requires {required_size} bytes, but only {data_left} bytes available"
            )));
        }

        let mut token_data: Vec<u8> = vec![0; data_left as usize];
        reader.read_bytes(&mut token_data[0..]).await?;

        let mut sts_url = String::new();
        let mut spn = String::new();
        for i in 0..options_count {
            let current_options_offset = i * Self::FEDAUTH_OPTIONS_SIZE;
            let option_id = token_data[current_options_offset as usize];

            // Validate slice bounds before conversion
            let length_slice = token_data
                .get((current_options_offset + 1) as usize..(current_options_offset + 5) as usize)
                .ok_or_else(|| {
                    Error::new(
                        std::io::ErrorKind::InvalidData,
                        "FedAuth option data length out of bounds",
                    )
                })?;
            let option_data_length = u32::from_le_bytes(length_slice.try_into().map_err(|_| {
                Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid FedAuth option data length",
                )
            })?);

            let offset_slice = token_data
                .get((current_options_offset + 5) as usize..(current_options_offset + 9) as usize)
                .ok_or_else(|| {
                    Error::new(
                        std::io::ErrorKind::InvalidData,
                        "FedAuth option offset out of bounds",
                    )
                })?;
            let mut option_data_offset =
                u32::from_le_bytes(offset_slice.try_into().map_err(|_| {
                    Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid FedAuth option offset",
                    )
                })?);

            option_data_offset -= size_of::<u32>() as u32;
            let string_bytes: &[u8] = token_data
                .get(
                    option_data_offset as usize..(option_data_offset + option_data_length) as usize,
                )
                .ok_or_else(|| {
                    Error::new(
                        std::io::ErrorKind::InvalidData,
                        "FedAuth string data out of bounds",
                    )
                })?;
            let u16_slice: Vec<u16> = string_bytes
                .chunks_exact(2)
                .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                .collect();
            let value = String::from_utf16(&u16_slice).map_err(|_| {
                Error::new(std::io::ErrorKind::InvalidData, "Invalid UTF-16 sequence")
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
                FedAuthInfoId::Unknown(id) => {
                    tracing::debug!(
                        "Ignoring unknown FedAuthInfoId: 0x{:02X} with value: {}",
                        id,
                        value
                    );
                }
            }
        }

        Ok(Tokens::from(FedAuthInfoToken { spn, sts_url }))
    }
}
