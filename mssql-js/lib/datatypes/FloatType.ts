// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';

export class FloatType extends DataType {
  constructor() {
    super(JsSqlDataTypes.Float);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    return typeof value === 'number';
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
  ): unknown {
    if (value === null) return null;
    if (typeof value === 'number') {
      const buffer = Buffer.alloc(8);
      buffer.writeDoubleLE(value, 0);
      return buffer;
    }
    throw new TypeError('Expected a number for Float/Real types');
  }
}
