// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { DataType } from './DataType';
import { JsSqlDataTypes } from './enums';

export class BigCharType extends DataType {
  constructor() {
    super(JsSqlDataTypes.BigChar);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    return typeof value === 'string';
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
  ): unknown {
    return value;
  }
}
