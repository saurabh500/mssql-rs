// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';

export class BinaryType extends DataType {
  private _length: number;
  static readonly maxLength = 8000;

  constructor(length: number) {
    super(JsSqlDataTypes.Binary);
    if (!(length >= 1 && length <= BinaryType.maxLength)) {
      throw new RangeError(
        `BinaryType length must be between 1 and ${BinaryType.maxLength}. Received: ${length}`,
      );
    }
    this._length = length;
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
    if (Buffer.isBuffer(value) && value.length === 0) return null;
    if (Buffer.isBuffer(value)) return value; // Return the Buffer directly
    throw new TypeError(
      `Expected a Buffer for VarBinary, but got ${typeof value}`,
    );
  }

  length(): number {
    return this._length;
  }
}
