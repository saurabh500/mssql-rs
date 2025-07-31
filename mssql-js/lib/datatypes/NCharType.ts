// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import { nCharNVarCharTdsTransformer } from '../transformers/string';
import { Encoding } from '../codepages';

export class NCharType extends DataType {
  constructor(public length: number) {
    super(JsSqlDataTypes.NChar);
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
}
