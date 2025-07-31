// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import type { Encoding } from '../codepages';
import { fromJsToNapiDateTransformer } from '../transformers/datetime';

export class DateType extends DataType {
  constructor() {
    super(JsSqlDataTypes.Date);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    if (value === null) {
      return true; // Allow null values
    }
    if (!(value instanceof Date)) {
      throw new TypeError(
        `Expected a Date for DateType, but got ${typeof value}`,
      );
    }
    return true;
  }

  /**
   * Transforms a JavaScript Date object to a format suitable for NAPI writes.
   * @param value The Date object to transform.
   * @param _encoding Optional encoding parameter, not used in this implementation.
   * @returns The transformed date value for NAPI writes.
   */
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
    _encoding?: Encoding,
  ): unknown {
    this.validate(value);
    return fromJsToNapiDateTransformer(value as Date | null);
  }
}
