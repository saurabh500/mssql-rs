// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { DataType } from './DataType';
import { JsSqlDataTypes } from './enums';
import type { Encoding } from '../codepages';

export class BigVarCharType extends DataType {
  constructor() {
    super(JsSqlDataTypes.BigVarChar);
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
