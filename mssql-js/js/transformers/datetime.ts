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

export interface DateWithNanosecondsDelta extends Date {
  nanosecondsDelta: number;
}

export const fromNapiToJsSmallDateTimeTransformer = (
  metadata: Metadata,
  row: NapiSqlDateTime | null,
): Date | null => {
  if (!row) return null;
  const { days, time } = row;
  return new Date(1900, 0, 1 + days, 0, time);
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

  // Lets say we get 1234567 from SQL with 7 scale, this means 0.123 seconds or 123 millis 456 micros and 700 nanos.
  // Lets say we get 1234567 from SQL with 5 scale, this means 12 seconds or 345 millis 670 micros and 0 nanos.
  let received_time = row.timeNanoseconds;
  // Convert timeNanoseconds to milliseconds.
  // Scale 7: 1234567 * 10 ^ (7-7) -> 1234567 * 10^0 = 001234567
  // Scale 5: 1234567 * 10 ^ (7-5) -> 1234567 * 10^2 = 123456700
  let normalize_time = Number(received_time) * 10 ** (7 - scale);

  // Extract the milliseconds.
  // Scale 7:  001234567 / 10000 = 123.4567 -> 123 millis
  // Scale 5: 123456700 / 10000 = 12345.67 -> 12345 millis which is 12 seconds and 345 millis
  let millis = Number(normalize_time) / 10_000; // Convert nanoseconds to milliseconds

  // Extract nanoseconds precision
  // Scale 7:  001234567 % 10000 -> 4567 / 10_000_000 = 0.0004567
  // Scale 5:  123456700 % 10000 -> 6700 / 10_000_000 = 0.00067
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

  // We extract the millis from the date and create a number which repreents the input date in millis.
  let millis = seconds * 1000 + time.getUTCMilliseconds();

  // Millis by default have scale 3. Adjust the number based on the intended scale.
  // E.g. If 1234567 millis are to be sent with scale 5, we need to multiply it by 10^(5-3) = 100, which gives us 123456700.
  // If the scale is 2 then we need to multiply it by 10^(2-3) = 0.1, which gives us 123456.7.
  let timeToSend = millis * Math.pow(10, scale - 3);
  timeToSend = Math.round(timeToSend); // Round to avoid floating point issues
  return {
    scale: scale,
    timeNanoseconds: BigInt(timeToSend),
  };
};
