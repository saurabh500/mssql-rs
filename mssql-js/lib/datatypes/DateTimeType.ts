// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { DataType } from './DataType';
import { JsSqlDataTypes } from './enums';
import type { Encoding } from '../codepages';
import { fromJsToNapiDateTimeTransformer } from '../transformers/datetime';

export class DateTimeType extends DataType {
  constructor() {
    super(JsSqlDataTypes.DateTime);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    if (value === null) {
      return true; // Allow null values
    }
    if (!(value instanceof Date)) {
      throw new TypeError(
        `Expected a Date for DateTimeType, but got ${typeof value}`,
      );
    }
    return true;
  }

  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
    _encoding?: Encoding,
  ): unknown {
    this.validate(value);
    return fromJsToNapiDateTimeTransformer(value as Date | null);
  }
}
