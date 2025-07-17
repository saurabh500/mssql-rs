// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { Metadata } from '../generated/index.js';

export const intTransformer = (
  metadata: Metadata,
  row: number | null,
): number | null => {
  return row == null ? null : row;
};

export const bigintTransformer = (
  metadata: Metadata,
  row: bigint | null,
): bigint | null => {
  return row == null ? null : row;
};
