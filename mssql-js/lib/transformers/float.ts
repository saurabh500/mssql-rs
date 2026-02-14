// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { Metadata } from '../generated/index.js';

export const floatTransformer = (
  metadata: Metadata,
  row: Buffer | null,
): number | null => {
  if (!row || row.length === 0) {
    return null;
  }
  if (row.length === 4) {
    return row.readFloatLE(0);
  }
  if (row.length === 8) {
    return row.readDoubleLE(0);
  }
  throw new Error(
    `Incorrect buffer size for float: received ${row.length} bytes`,
  );
};
