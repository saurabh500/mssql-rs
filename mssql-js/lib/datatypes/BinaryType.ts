// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import type { Encoding } from '../codepages';

export class BinaryType extends DataType {
  constructor(public length: number) {
    super(JsSqlDataTypes.Binary);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    if (value === null) return true; // Allow null values
    if (Buffer.isBuffer(value)) return true;
    throw new TypeError(
      `Expected a Buffer for VarBinary, but got ${typeof value}`,
    );
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
  ): unknown {
    this.validate(value);
    if (value === null) return null; // Handle null case
    if (Buffer.isBuffer(value)) return value; // Return the Buffer directly
    throw new TypeError(
      `Expected a Buffer for VarBinary, but got ${typeof value}`,
    );
  }
}
