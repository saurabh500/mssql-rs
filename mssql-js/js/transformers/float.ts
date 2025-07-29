// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { Metadata, NapiF64 } from '../generated/index.js';

export const floatTransformer = (
  metadata: Metadata,
  row: NapiF64 | null,
): number | null => {
  return row == null ? null : row.value;
};
