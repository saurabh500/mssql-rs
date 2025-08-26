// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { DataType } from './DataType';
import type { Encoding } from '../codepages';
import { fromJsToNapiDatetime2Transformer } from '../transformers/datetime';
import { JsSqlDataTypes } from './enums';

export class DateTime2Type extends DataType {
  constructor(public scale: number) {
    super(JsSqlDataTypes.DateTime2);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    if (value === null) {
      return true; // Allow null values
    }
    if (!(value instanceof Date)) {
      throw new TypeError(
        `Expected a Date for Dateime2Type, but got ${typeof value}`,
      );
    }
    return true;
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
  ): unknown {
    this.validate(value);
    return fromJsToNapiDatetime2Transformer(value as Date | null, this.scale);
  }
}
