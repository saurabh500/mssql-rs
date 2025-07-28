// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { Metadata } from '../generated/index.js';

export const fromNapiToJsBitTransformer = (
  metadata: Metadata,
  row: boolean | null,
): boolean | null => {
  return row == null ? null : row;
};
