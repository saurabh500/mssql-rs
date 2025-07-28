// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { Metadata, NapiDecimalParts } from '../generated/index.js';

export const fromNapiToJsDecimalTransformer = (
  metadata: Metadata,
  row: NapiDecimalParts | null,
): number | null => {
  const decimal_parts = row;
  if (decimal_parts != null) {
    const sign = decimal_parts.isPositive ? 1 : -1;
    const parts = decimal_parts.intParts;
    let value = 0;
    for (let i = 0; i < parts.length; i++) {
      value += (parts[i] >>> 0) * Math.pow(0x100000000, i);
    }
    const scale = decimal_parts.scale;
    value = value / Math.pow(10, scale);
    return value * sign;
  }
  return null;
};
