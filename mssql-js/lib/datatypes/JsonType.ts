// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import type { Encoding } from '../codepages';

export class JsonType extends DataType {
  constructor() {
    super(JsSqlDataTypes.Json);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    return typeof value === 'string' || typeof value === 'object';
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
    encoding?: Encoding,
  ): unknown {
    return value;
  }
}
