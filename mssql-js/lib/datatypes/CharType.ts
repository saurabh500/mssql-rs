// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import type { Encoding } from '../codepages';

export class CharType extends DataType {
  static readonly maxLength = 4000;
  private _length: number;

  constructor(length: number) {
    super(JsSqlDataTypes.Char);
    if (!(length >= 1 && length <= CharType.maxLength)) {
      throw new RangeError(
        `CharType length must be between 1 and ${CharType.maxLength}. Received: ${length}`,
      );
    }
    this._length = length;
  }

  validate(value: bigint | number | string | Date | boolean | null): boolean {
    return typeof value === 'string' && value.length <= this.length();
  }

  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
    _encoding?: Encoding,
  ): unknown {
    if (typeof value === 'string' && value.length > this.length()) {
      return value.substring(0, this.length());
    }
    return value;
  }

  length(): number {
    return this._length;
  }
}
