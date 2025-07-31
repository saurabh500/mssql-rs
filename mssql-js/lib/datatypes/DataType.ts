// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { JsSqlDataTypes } from '../.';
import type { Encoding } from '../codepages';

export abstract class DataType {
  constructor(public sqlType: JsSqlDataTypes) {}
  abstract validate(
    value: bigint | number | string | Date | boolean | null,
  ): boolean;
  abstract transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
    encoding?: Encoding,
  ): unknown;
}
