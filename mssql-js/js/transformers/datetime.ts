// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import {
  Metadata,
  NapiSqlDateTime,
  NapiSqlDateTime2,
  NapiSqlTime,
  NapiSqlDateTimeOffset,
} from '../generated/index.js';

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
  return new Date(1900, 0, 1 + days, 0, 0, 0, milliseconds);
};

export const dateTransformer = (
  metadata: Metadata,
  row: number | null,
): Date | null => {
  if (row == null) return null;
  const numdaysbetween010101and20000101 = 730118;
  return new Date(2000, 0, row - numdaysbetween010101and20000101);
};

export const dateTime2Transformer = (
  metadata: Metadata,
  row: NapiSqlDateTime2 | null,
): Date | null => {
  if (!row) return null;
  const { days } = row;
  const baseDate = new Date(2000, 0, 1);
  const date = new Date(baseDate.getTime() + days * 24 * 60 * 60 * 1000);
  return date;
};

export const dateTimeOffsetTransformer = (
  metadata: Metadata,
  row: NapiSqlDateTimeOffset | null,
): NapiSqlDateTimeOffset | null => {
  if (!row) return null;
  return row;
};

export const timeTransformer = (
  metadata: Metadata,
  row: NapiSqlTime | null,
): NapiSqlTime | null => {
  if (!row) return null;
  return row;
};
