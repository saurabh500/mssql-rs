// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import { nCharNVarCharTdsTransformer } from '../transformers/string';
import { Encoding } from '../codepages';

export class NCharType extends DataType {
  private _length: number;
  static readonly maxLength = 4000;

  constructor(length: number) {
    super(JsSqlDataTypes.NChar);
    if (!(length >= 1 && length <= NCharType.maxLength)) {
      throw new RangeError(
        `NCharType length must be between 1 and ${NCharType.maxLength}. Received: ${length}`,
      );
    }
    this._length = length;
  }

  validate(value: bigint | number | string | Date | boolean | null): boolean {
    if (value === null) {
      return true; // Allow null values
    }
    if (typeof value !== 'string') {
      throw new TypeError(
        `Expected a string for NChar, but got ${typeof value}`,
      );
    }
    return true;
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
    encoding?: Encoding,
  ): unknown {
    this.validate(value);
    return nCharNVarCharTdsTransformer(value as string | null, encoding);
  }

  length(): number {
    return this._length;
  }
}
