// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { Metadata } from '../generated/index.js';

export const guidTransformer = (
  metadata: Metadata,
  row: Buffer | null,
): string | null => {
  if (row == null) return null;
  const guid_buff = row;
  return guid_buff == null ? null : guid_buff.toString('utf8');
};
