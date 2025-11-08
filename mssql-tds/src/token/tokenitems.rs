// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturnValueStatus {
    OutputParam,
    Udf,
    Unknown(u8),
}

impl From<u8> for ReturnValueStatus {
    fn from(value: u8) -> Self {
        match value {
            0x01 => ReturnValueStatus::OutputParam,
            0x02 => ReturnValueStatus::Udf,
            _ => {
                tracing::warn!("Unknown ReturnValueStatus value: 0x{:02X}", value);
                ReturnValueStatus::Unknown(value)
            }
        }
    }
}
