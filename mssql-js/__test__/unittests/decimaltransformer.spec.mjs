// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';
import { fromJsToNapiDecimalPartTransformer } from '../../dist/transformers/decimal.js';

test('fromJsToNapiDecimalPartTransformer converts number to NapiDecimalParts', (t) => {
  const input = 32.456;
  const expected = {
    isPositive: true,
    intParts: [32456],
    scale: 3,
    precision: 6,
  };
  const result = fromJsToNapiDecimalPartTransformer(input, 3, 6);
  t.deepEqual(result, expected, 'Expected NapiDecimalParts to match');
});

test('fromJsToNapiDecimalPartTransformer scale 3, precision 38, converts number to NapiDecimalParts', (t) => {
  const input = 32.456;
  const precision = 38;
  const scale = 3;
  const expected = {
    isPositive: true,
    intParts: [32456, 0, 0, 0],
    scale: scale,
    precision: precision,
  };
  const result = fromJsToNapiDecimalPartTransformer(input, scale, precision);
  t.deepEqual(result, expected, 'Expected NapiDecimalParts to match');
});

test('fromJsToNapiDecimalPartTransformer scale 30, precision 38, converts number to NapiDecimalParts', (t) => {
  const input = -0.00000000000000000000000000012345000456;
  const precision = 38;
  const scale = 38;
  const expected = {
    isPositive: false,
    intParts: [-539901432, 2, 0, 0],
    scale: scale,
    precision: precision,
  };
  const result = fromJsToNapiDecimalPartTransformer(input, scale, precision);
  t.deepEqual(result, expected, 'Expected NapiDecimalParts to match');
});

test('fromJsToNapiDecimalPartTransformer scale 8, precision 18, converts muti int number to NapiDecimalParts', (t) => {
  const input = 12323123123.45;
  const precision = 18;
  const scale = 0;
  const expected = {
    isPositive: true,
    intParts: [-561778765, 2],
    scale: scale,
    precision: precision,
  };
  const result = fromJsToNapiDecimalPartTransformer(input, scale, precision);
  t.deepEqual(result, expected, 'Expected NapiDecimalParts to match');
});
