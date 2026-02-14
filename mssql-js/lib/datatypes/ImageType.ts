// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { DataType } from './DataType';
import { JsSqlDataTypes } from './enums';
import type { Encoding } from '../codepages';

export class ImageType extends DataType {
  constructor() {
    super(JsSqlDataTypes.Image);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    return true;
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
    encoding?: Encoding,
  ): unknown {
    return value;
  }
}
