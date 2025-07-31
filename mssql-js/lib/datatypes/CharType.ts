// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import type { Encoding } from '../codepages';

export class CharType extends DataType {
  constructor(public length: number) {
    super(JsSqlDataTypes.Char);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    return typeof value === 'string' && value.length <= this.length;
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
    encoding?: Encoding,
  ): unknown {
    if (typeof value === 'string' && value.length > this.length) {
      return value.substring(0, this.length);
    }
    return value;
  }
}
