// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use mssql_tds::datatypes::column_values::{
    SqlDate, SqlDateTime, SqlDateTime2, SqlDateTimeOffset, SqlSmallDateTime, SqlTime,
};

/// Temporal value combining all TDS date/time components.
///
/// Fields are optional because different SQL Server types carry different
/// subsets of date, time, and offset information.
#[derive(Debug, Clone, PartialEq)]
pub struct DateTime {
    pub year: Option<i32>,
    pub month: Option<u8>,
    pub day: Option<u8>,
    pub hour: Option<u8>,
    pub minute: Option<u8>,
    pub second: Option<u8>,
    pub nanoseconds: Option<u32>,
    pub offset_minutes: Option<i16>,
}

// Base date for SQL Server's DATE/DATETIME2/DATETIMEOFFSET epoch: 0001-01-01
fn days_to_ymd(total_days: u32) -> (i32, u8, u8) {
    // Algorithm converts days since 0001-01-01 to (year, month, day).
    // Uses a well-known civil calendar algorithm.
    let y400 = total_days / 146_097;
    let mut remaining = total_days % 146_097;

    let mut y100 = remaining / 36_524;
    if y100 == 4 {
        y100 = 3;
    }
    remaining -= y100 * 36_524;

    let y4 = remaining / 1_461;
    remaining -= y4 * 1_461;

    let mut y1 = remaining / 365;
    if y1 == 4 {
        y1 = 3;
    }
    remaining -= y1 * 365;

    let year = (y400 * 400 + y100 * 100 + y4 * 4 + y1 + 1) as i32;
    let leap = (y1 == 3) && (y4 != 0 || y100 == 3);
    let feb = if leap { 29u32 } else { 28 };
    let month_days: [u32; 12] = [31, feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    let mut month = 0u8;
    for (i, &days_in_month) in month_days.iter().enumerate() {
        if remaining < days_in_month {
            month = (i + 1) as u8;
            break;
        }
        remaining -= days_in_month;
    }
    let day = (remaining + 1) as u8;
    (year, month, day)
}

fn nanos_to_hms(ticks_100ns: u64) -> (u8, u8, u8, u32) {
    // SqlTime.time_nanoseconds stores 100-nanosecond ticks, convert to actual nanoseconds
    let nanos = ticks_100ns * 100;
    let total_secs = nanos / 1_000_000_000;
    let sub_nanos = (nanos % 1_000_000_000) as u32;
    let hour = (total_secs / 3600) as u8;
    let minute = ((total_secs % 3600) / 60) as u8;
    let second = (total_secs % 60) as u8;
    (hour, minute, second, sub_nanos)
}

impl From<SqlDate> for DateTime {
    fn from(d: SqlDate) -> Self {
        let (year, month, day) = days_to_ymd(d.get_days());
        DateTime {
            year: Some(year),
            month: Some(month),
            day: Some(day),
            hour: None,
            minute: None,
            second: None,
            nanoseconds: None,
            offset_minutes: None,
        }
    }
}

impl From<SqlTime> for DateTime {
    fn from(t: SqlTime) -> Self {
        let (hour, minute, second, nanos) = nanos_to_hms(t.time_nanoseconds);
        DateTime {
            year: None,
            month: None,
            day: None,
            hour: Some(hour),
            minute: Some(minute),
            second: Some(second),
            nanoseconds: Some(nanos),
            offset_minutes: None,
        }
    }
}

impl From<SqlDateTime2> for DateTime {
    fn from(dt: SqlDateTime2) -> Self {
        let (year, month, day) = days_to_ymd(dt.days);
        let (hour, minute, second, nanos) = nanos_to_hms(dt.time.time_nanoseconds);
        DateTime {
            year: Some(year),
            month: Some(month),
            day: Some(day),
            hour: Some(hour),
            minute: Some(minute),
            second: Some(second),
            nanoseconds: Some(nanos),
            offset_minutes: None,
        }
    }
}

impl From<SqlDateTimeOffset> for DateTime {
    fn from(dto: SqlDateTimeOffset) -> Self {
        let (year, month, day) = days_to_ymd(dto.datetime2.days);
        let (hour, minute, second, nanos) = nanos_to_hms(dto.datetime2.time.time_nanoseconds);
        DateTime {
            year: Some(year),
            month: Some(month),
            day: Some(day),
            hour: Some(hour),
            minute: Some(minute),
            second: Some(second),
            nanoseconds: Some(nanos),
            offset_minutes: Some(dto.offset),
        }
    }
}

impl From<SqlSmallDateTime> for DateTime {
    fn from(sdt: SqlSmallDateTime) -> Self {
        // SmallDateTime: days since 1900-01-01, time in minutes
        // Convert to days since 0001-01-01: add days from 0001-01-01 to 1900-01-01
        // 1900-01-01 is day 693,595 from 0001-01-01
        let total_days = sdt.days as u32 + 693_595;
        let (year, month, day) = days_to_ymd(total_days);
        let total_minutes = sdt.time as u32;
        let hour = (total_minutes / 60) as u8;
        let minute = (total_minutes % 60) as u8;
        DateTime {
            year: Some(year),
            month: Some(month),
            day: Some(day),
            hour: Some(hour),
            minute: Some(minute),
            second: Some(0),
            nanoseconds: None,
            offset_minutes: None,
        }
    }
}

impl From<SqlDateTime> for DateTime {
    fn from(dt: SqlDateTime) -> Self {
        // DateTime: days since 1900-01-01 (i32), time in 1/300ths of a second (u32)
        let total_days = dt.days as i64 + 693_595;
        let (year, month, day) = days_to_ymd(total_days as u32);
        let total_millis = (dt.time as u64 * 10) / 3; // 1/300th sec → milliseconds
        let total_secs = total_millis / 1000;
        let remaining_millis = total_millis % 1000;
        let hour = (total_secs / 3600) as u8;
        let minute = ((total_secs % 3600) / 60) as u8;
        let second = (total_secs % 60) as u8;
        let nanos = (remaining_millis * 1_000_000) as u32;
        DateTime {
            year: Some(year),
            month: Some(month),
            day: Some(day),
            hour: Some(hour),
            minute: Some(minute),
            second: Some(second),
            nanoseconds: Some(nanos),
            offset_minutes: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_epoch() {
        let d = SqlDate::create(0).unwrap();
        let dt: DateTime = d.into();
        assert_eq!(dt.year, Some(1));
        assert_eq!(dt.month, Some(1));
        assert_eq!(dt.day, Some(1));
    }

    #[test]
    fn time_midnight() {
        let t = SqlTime {
            time_nanoseconds: 0,
            scale: 7,
        };
        let dt: DateTime = t.into();
        assert_eq!(dt.hour, Some(0));
        assert_eq!(dt.minute, Some(0));
        assert_eq!(dt.second, Some(0));
        assert_eq!(dt.nanoseconds, Some(0));
        assert!(dt.year.is_none());
    }

    #[test]
    fn smalldatetime_epoch() {
        let sdt = SqlSmallDateTime { days: 0, time: 0 };
        let dt: DateTime = sdt.into();
        assert_eq!(dt.year, Some(1900));
        assert_eq!(dt.month, Some(1));
        assert_eq!(dt.day, Some(1));
        assert_eq!(dt.hour, Some(0));
        assert_eq!(dt.minute, Some(0));
    }
}
