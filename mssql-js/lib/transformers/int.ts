// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { Metadata } from '../generated/index.js';

export const fromNapiToJsIntTransformer = (
  metadata: Metadata,
  row: number | null,
): number | null => {
  return row;
};

export const fromNapiToJsBigintTransformer = (
  metadata: Metadata,
  row: bigint | null,
): bigint | null => {
  return row;
};
