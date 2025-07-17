// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { Metadata } from '../generated/index.js';

export const guidTransformer = (
  metadata: Metadata,
  row: string | null,
): string | null => {
  return row;
};
