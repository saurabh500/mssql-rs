// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { Metadata } from '../generated/index.js';

export const bitTransformer = (
  metadata: Metadata,
  row: boolean | null,
): boolean | null => {
  return row == null ? null : row;
};
