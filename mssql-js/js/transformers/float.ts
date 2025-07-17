// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { Metadata } from '../generated/index.js';

export const floatTransformer = (
  metadata: Metadata,
  row: number | null,
): number | null => {
  return row == null ? null : row;
};
