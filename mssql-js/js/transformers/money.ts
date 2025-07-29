// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { Metadata, NapiSqlMoney } from '../generated/index.js';

export const moneyTransformer = (
  metadata: Metadata,
  row: NapiSqlMoney | null,
): number | null => {
  if (row == null) return null;
  const money_val = row;
  const lo = money_val.lsbPart >>> 0; // Convert to unsigned 32-bit integer.
  const hi = money_val.msbPart * 0x100000000;
  return (lo + hi) / 10_000;
};

export const smallMoneyTransformer = (
  metadata: Metadata,
  row: number | null,
): number | null => {
  if (row == null) return null;
  return row / 10_000;
};

export const fromJsToSmallMoneyTransformer = (
  value: number | null,
): number | null => {
  if (value == null) return null;
  const scaledValue = Math.round(value * 10_000);
  return scaledValue;
};

export const fromJsToNapiMoneyTransformer = (
  value: number | null,
): NapiSqlMoney | null => {
  if (value == null) return null;
  const scaledValue = Math.round(value * 10_000);
  const msbPart = Math.floor(scaledValue * (1 / ((1 << 16) * (1 << 16))));
  const lsbPart = (scaledValue & -1) << 0; // Ensure it's a signed 32-bit integer.
  return { lsbPart, msbPart };
};
