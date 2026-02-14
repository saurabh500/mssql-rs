// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { ChronoUnit, LocalDate, Month } from '@js-joda/core';
import {
  Metadata,
  NapiSqlDateTime,
  NapiSqlDateTime2,
  NapiSqlTime,
  NapiSqlDateTimeOffset,
} from '../generated/index.js';

const SQL_EPOCH_DATE = LocalDate.of(1, Month.JANUARY, 1);
const SQL_1900_EPOCH_DATE = LocalDate.of(1900, Month.JANUARY, 1);

export interface DateWithNanosecondsDelta extends Date {
  nanosecondsDelta: number;
}

export const fromNapiToJsSmallDateTimeTransformer = (
  metadata: Metadata,
  row: NapiSqlDateTime | null,
): Date | null => {
  if (!row) return null;
  const { days, time } = row;
  return new Date(Date.UTC(1900, 0, 1 + days, 0, time));
};

export const fromNapiToJsDateTimeTransformer = (
  metadata: Metadata,
  row: NapiSqlDateTime | null,
): Date | null => {
  if (!row) return null;
  const { days, time } = row;
  const milliseconds = Math.round(time * (3 + 1 / 3));
  return new Date(Date.UTC(1900, 0, 1 + days, 0, 0, 0, milliseconds));
};

export const fromNapiToJsDateTransformer = (
  metadata: Metadata,
  daysSince010101: number | null,
): Date | null => {
  if (daysSince010101 == null) return null;
  const daysCountBetween010101And20000101 = 730118;
  return new Date(
    Date.UTC(2000, 0, daysSince010101 - daysCountBetween010101And20000101),
  );
};

export const fromJsToNapiDateTransformer = (
  date: Date | null,
): number | null => {
  if (!date) return null;
  let local_date = LocalDate.of(
    date.getUTCFullYear(),
    date.getUTCMonth() + 1,
    date.getUTCDate(),
  );
  return SQL_EPOCH_DATE.until(local_date, ChronoUnit.DAYS);
};

export const fromNapiToJsDatetime2Transformer = (
  metadata: Metadata,
  row: NapiSqlDateTime2 | null,
): DateWithNanosecondsDelta | null => {
  if (!row) return null;

  let local_date = SQL_EPOCH_DATE.plusDays(row.days);

  // time_part is guaranteed not to be null
  const time_part = fromNapiToJsTimeTransformer(metadata, row.time)!;

  const date = new Date(
    Date.UTC(
      local_date.year(),
      local_date.monthValue() - 1,
      local_date.dayOfMonth(),
      0,
      0,
      0,
      // Time is a date since epoch with the time added to it. Hence it will be coerced to number of millis since epoch,
      // giving us only the time part since epoch. We use this fact to add the time part to the date.
      +time_part,
    ),
  );

  (date as DateWithNanosecondsDelta).nanosecondsDelta =
    time_part.nanosecondsDelta;
  return date as DateWithNanosecondsDelta;
};

export const fromJsToNapiDatetime2Transformer = (
  row: Date | null,
  scale: number = 7,
): NapiSqlDateTime2 | null => {
  if (!row) return null;
  let sqlTime = fromJsToNapiTimeTransformer(row, scale);
  let daysSince010101 = fromJsToNapiDateTransformer(row);
  return {
    days: daysSince010101!,
    time: sqlTime!,
  };
};

export const fromNapiToJsDateTimeOffsetTransformer = (
  metadata: Metadata,
  row: NapiSqlDateTimeOffset | null,
): DateWithNanosecondsDelta | null => {
  if (!row) return null;

  let datetime2 = fromNapiToJsDatetime2Transformer(metadata, row.datetime2);
  // We discard the offset, since the time returned by SQL server is always in UTC.
  // Offset is meant to be used for display purposes only.
  return datetime2;
};

export const fromJsToNapiDateTimeOffsetTransformer = (
  row: Date | null,
  scale: number = 7,
): NapiSqlDateTimeOffset | null => {
  if (!row) return null;
  let datetime2 = fromJsToNapiDatetime2Transformer(row, scale);
  let offset = row.getTimezoneOffset();
  return {
    datetime2: datetime2!,
    offset: offset,
  };
};

/// Transform the NapiSqlTime to a Date object
export const fromNapiToJsTimeTransformer = (
  metadata: Metadata,
  row: NapiSqlTime | null,
): DateWithNanosecondsDelta | null => {
  if (!row) return null;
  let scale = row.scale;
  // Normalize to 7 scale.
  if (scale < 0 || scale > 7) {
    throw new Error(`Invalid scale: ${scale}. Must be between 0 and 7.`);
  }

  // The timeNanoseconds field is already in 100-nanosecond units after deserialization from Rust.
  // No need to apply additional scaling here.
  let time_in_100ns = Number(row.timeNanoseconds);

  // Convert 100-nanoseconds to milliseconds by dividing by 10,000
  let millis = time_in_100ns / 10_000;

  // Extract nanoseconds precision for sub-millisecond accuracy
  // The remainder after converting to millis represents the sub-millisecond nanoseconds
  let nanos_precision = (time_in_100ns % 10_000) / Math.pow(10, 7);

  // Create a Date object starting from the epoch (1970-01-01)
  // and add the milliseconds to it.
  // Note: JavaScript Date uses UTC, so we can safely use UTC methods.
  // The time part is represented as UTC, so we can directly use it.
  let datePart = new Date(Date.UTC(1970, 0, 1, 0, 0, 0, millis));
  (datePart as DateWithNanosecondsDelta).nanosecondsDelta = nanos_precision;
  return datePart as DateWithNanosecondsDelta;
};

/// Transform the NapiSqlTime to a Date object
export const fromJsToNapiTimeTransformer = (
  time: Date | null,
  scale: number = 7,
): NapiSqlTime | null => {
  if (!time) return null;

  // Normalize to 7 scale.
  if (scale < 0 || scale > 7) {
    throw new Error(`Invalid scale: ${scale}. Must be between 0 and 7.`);
  }
  
  let seconds =
    (time.getUTCHours() * 60 + time.getUTCMinutes()) * 60 +
    time.getUTCSeconds();

  // We extract the millis from the date
  let millis = seconds * 1000 + time.getUTCMilliseconds();

  // Convert milliseconds to 100-nanosecond units (scale 7).
  // 1 millisecond = 10,000 * 100-nanoseconds
  let timeIn100ns = millis * 10_000;
  
  // Round to the scale precision if needed.
  // This ensures values are rounded rather than truncated when they exceed scale precision.
  if (scale < 7) {
    const divisor = 10 ** (7 - scale);
    const halfDivisor = Math.floor(divisor / 2);
    timeIn100ns = Math.floor((timeIn100ns + halfDivisor) / divisor) * divisor;
  }
  
  return {
    scale: scale,
    timeNanoseconds: BigInt(timeIn100ns),
  };
};

export const fromJsToNapiDateTimeTransformer = (
  date: Date | null,
): NapiSqlDateTime | null => {
  if (!date) return null;
  let local_date = LocalDate.of(
    date.getUTCFullYear(),
    date.getUTCMonth() + 1,
    date.getUTCDate(),
  );
  let days = SQL_1900_EPOCH_DATE.until(local_date, ChronoUnit.DAYS);
  let millis =
    date.getUTCHours() * 3_600 * 1_000 + // Hours to millis
    date.getUTCMinutes() * 60 * 1_000 + // Minutes to millis
    date.getUTCSeconds() * 1_000 + // Seconds to millis
    date.getUTCMilliseconds(); // Millis
  let time = Math.round((millis / 10) * 3); // Convert milliseconds to  1/300th of seconds
  return {
    days: days,
    time: time,
  } as NapiSqlDateTime;
};

export const fromJsToNapiSmallDateTimeTransformer = (
  date: Date | null,
): NapiSqlDateTime | null => {
  if (!date) return null;
  let local_date = LocalDate.of(
    date.getUTCFullYear(),
    date.getUTCMonth() + 1,
    date.getUTCDate(),
  );
  let days = SQL_1900_EPOCH_DATE.until(local_date, ChronoUnit.DAYS);
  if (days < 0) {
    throw new Error('Date cannot be before 1900-01-01 for SmallDateTime');
  }
  let minutes =
    date.getUTCHours() * 60 + // Hours to minutes
    date.getUTCMinutes();

  return {
    days: days,
    time: minutes,
  } as NapiSqlDateTime;
};
