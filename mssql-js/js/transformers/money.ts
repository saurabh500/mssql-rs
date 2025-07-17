// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { Metadata, NapiSqlMoney } from '../generated/index.js';

export const moneyTransformer = (
  metadata: Metadata,
  row: NapiSqlMoney | null,
): number | null => {
  if (row == null) return null;
  const money_val = row;
  const lo = money_val.lsbPart;
  const hi = money_val.msbPart;
  return (lo + 0x100000000 * hi) / 10000;
};

export const smallMoneyTransformer = (
  metadata: Metadata,
  row: number | null,
): number | null => {
  if (row == null) return null;
  return row / 10000;
};
