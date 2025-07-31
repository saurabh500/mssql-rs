// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import type { Encoding } from '../codepages';

export class BigVarBinaryType extends DataType {
  constructor() {
    super(JsSqlDataTypes.BigVarBinary);
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
