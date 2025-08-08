// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { DataType } from './DataType';
import { JsSqlDataTypes } from './enums';
import type { Encoding } from '../codepages';

export class BitType extends DataType {
  constructor() {
    super(JsSqlDataTypes.Bit);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    throw new Error('BitType does not support validation.');
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
  ): unknown {
    if (value === null) return null;
    if (typeof value === 'boolean') return value;
    if (typeof value === 'number') return Boolean(value);
    if (typeof value === 'string') return value === 'true' || value === '1';
    throw new TypeError('Expected a boolean, number, or string for BitType');
  }
}
