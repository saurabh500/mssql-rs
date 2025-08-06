// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import type { Encoding } from '../codepages';

export class UniqueIdentifierType extends DataType {
  constructor() {
    super(JsSqlDataTypes.UniqueIdentifier);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    return typeof value === 'string';
  }

  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
    encoding?: Encoding,
  ): unknown {
    if (typeof value === 'string' || value === null) return value;
    throw new TypeError('Expected a string for UniqueIdentifier');
  }
}
