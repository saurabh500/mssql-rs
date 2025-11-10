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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_return_value_status_from_output_param() {
        assert_eq!(
            ReturnValueStatus::from(0x01),
            ReturnValueStatus::OutputParam
        );
    }

    #[test]
    fn test_return_value_status_from_udf() {
        assert_eq!(ReturnValueStatus::from(0x02), ReturnValueStatus::Udf);
    }

    #[test]
    fn test_return_value_status_from_unknown() {
        assert_eq!(
            ReturnValueStatus::from(0xFF),
            ReturnValueStatus::Unknown(0xFF)
        );
        assert_eq!(
            ReturnValueStatus::from(0x00),
            ReturnValueStatus::Unknown(0x00)
        );
        assert_eq!(
            ReturnValueStatus::from(0x03),
            ReturnValueStatus::Unknown(0x03)
        );
    }

    #[test]
    fn test_return_value_status_equality() {
        assert_eq!(
            ReturnValueStatus::OutputParam,
            ReturnValueStatus::OutputParam
        );
        assert_eq!(ReturnValueStatus::Udf, ReturnValueStatus::Udf);
        assert_ne!(ReturnValueStatus::OutputParam, ReturnValueStatus::Udf);
    }

    #[test]
    fn test_return_value_status_clone() {
        let status = ReturnValueStatus::OutputParam;
        let cloned = status.clone();
        assert_eq!(status, cloned);
    }

    #[test]
    fn test_return_value_status_debug() {
        let status = ReturnValueStatus::OutputParam;
        let debug_str = format!("{:?}", status);
        assert!(debug_str.contains("OutputParam"));
    }
}
