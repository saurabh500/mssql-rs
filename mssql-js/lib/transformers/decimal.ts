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

// Converts a JavaScript number to a NAPI decimal representation.
// The max size of JS number is 2^53 - 1, which is less than the max value of a SQL decimal (38 digits).
// Hence we need to care about sending the 2 integer parts.
export const fromJsToNapiDecimalPartTransformer = (
  value: number | null,
  scale: number = 0,
  precision: number = 38,
): NapiDecimalParts | null => {
  if (precision < 1 || precision > 38) {
    throw new TypeError(
      `Precision ${precision} is not supported for decimal. Max is 38.`,
    );
  }
  if (scale > precision) {
    throw new TypeError(
      `Scale ${scale} cannot be greater than precision ${precision}.`,
    );
  }

  if (value == null) return null;
  let scaledValue = Math.abs(Math.round(value * Math.pow(10, scale)));
  const isPositive = value >= 0;
  const intParts: number[] = [];
  if (precision <= 9) {
    // One part
    intParts.push(scaledValue & 0xffffffff);
  } else if (precision <= 18) {
    // Two parts
    intParts.push(scaledValue & 0xffffffff);
    intParts.push((scaledValue / 2 ** 32) << 0); // Use bitwise shift to ensure it's a 32-bit integer and make it signed.
  } else if (precision <= 27) {
    // Three parts
    intParts.push(scaledValue & 0xffffffff);
    intParts.push((scaledValue / 2 ** 32) << 0);
    intParts.push(0);
  } else if (precision <= 38) {
    // Four parts
    intParts.push(scaledValue & 0xffffffff);
    intParts.push((scaledValue / 2 ** 32) << 0);
    intParts.push(0);
    intParts.push(0);
  } else {
    throw new TypeError(
      `Precision ${precision} is not supported for decimal. Max is 38.`,
    );
  }

  return {
    isPositive,
    intParts,
    precision,
    scale,
  };
};
