// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { DataType } from './DataType';
import { JsSqlDataTypes } from './enums';

export class BigBinaryType extends DataType {
  constructor() {
    super(JsSqlDataTypes.BigBinary);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    return value instanceof Uint8Array || value === null;
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
  ): unknown {
    return value;
  }
}
