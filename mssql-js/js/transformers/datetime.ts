// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import {
  Metadata,
  NapiSqlDateTime,
  NapiSqlDateTime2,
  NapiSqlTime,
  NapiSqlDateTimeOffset,
} from '../generated/index.js';

export interface DateWithNanosecondsDelta extends Date {
  nanosecondsDelta: number;
}

export const smallDateTimeTransformer = (
  metadata: Metadata,
  row: NapiSqlDateTime | null,
): Date | null => {
  if (!row) return null;
  const { days, time } = row;
  return new Date(1900, 0, 1 + days, 0, time);
};

export const dateTimeTransformer = (
  metadata: Metadata,
  row: NapiSqlDateTime | null,
): Date | null => {
  if (!row) return null;
  const { days, time } = row;
  const milliseconds = Math.round(time * (3 + 1 / 3));
  return new Date(Date.UTC(1900, 0, 1 + days, 0, 0, 0, milliseconds));
};

export const dateTransformer = (
  metadata: Metadata,
  daysSince010101: number | null,
): Date | null => {
  if (daysSince010101 == null) return null;
  const daysCountBetween010101And20000101 = 730118;
  return new Date(
    Date.UTC(2000, 0, daysSince010101 - daysCountBetween010101And20000101),
  );
};

export const dateTime2Transformer = (
  metadata: Metadata,
  row: NapiSqlDateTime2 | null,
): DateWithNanosecondsDelta | null => {
  if (!row) return null;
  let daysSince010101 = row.days;
  const daysCountBetween010101And20000101 = 730118;

  // time_part is guaranteed not to be null
  const time_part = timeTransformer(metadata, row.time)!;

  const date = new Date(
    Date.UTC(
      2000,
      0,
      daysSince010101 - daysCountBetween010101And20000101,
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

export const dateTimeOffsetTransformer = (
  metadata: Metadata,
  row: NapiSqlDateTimeOffset | null,
): DateWithNanosecondsDelta | null => {
  if (!row) return null;

  let datetime2 = dateTime2Transformer(metadata, row.datetime2);
  // We discard the offset, since the time returned by SQL server is always in UTC.
  // Offset is meant to be used for display purposes only.
  return datetime2;
};

/// Transform the NapiSqlTime to a Date object
export const timeTransformer = (
  metadata: Metadata,
  row: NapiSqlTime | null,
): DateWithNanosecondsDelta | null => {
  if (!row) return null;
  let scale = row.scale;
  // Normalize to 7 scale.
  if (scale < 0 || scale > 7) {
    throw new Error(`Invalid scale: ${scale}. Must be between 0 and 7.`);
  }
  // Convert timeNanoseconds to milliseconds.
  let received_time = row.timeNanoseconds;
  let normalize_time = Number(received_time) * 10 ** (7 - scale);
  let millis = Number(normalize_time) / 10_000; // Convert nanoseconds to milliseconds

  // Extract nanoseconds precision
  let nanos_precision = (normalize_time % 10_000) / Math.pow(10, 7);

  // Create a Date object starting from the epoch (1970-01-01)
  // and add the milliseconds to it.
  // Note: JavaScript Date uses UTC, so we can safely use UTC methods.
  // The epoch for SQL Server is 1900-01-01, but we start from 1970-01-01
  // and adjust the date accordingly.
  // The time part is represented as UTC, so we can directly use it.
  // The date part is not used here, as we are only interested in the time.
  let datePart = new Date(Date.UTC(1970, 0, 1, 0, 0, 0, millis));
  (datePart as DateWithNanosecondsDelta).nanosecondsDelta = nanos_precision;
  return datePart as DateWithNanosecondsDelta;
};
