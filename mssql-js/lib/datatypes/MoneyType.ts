// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import type { Encoding } from '../codepages';
import { fromJsToNapiMoneyTransformer } from '../transformers/money';

export class MoneyType extends DataType {
  constructor() {
    super(JsSqlDataTypes.Money);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    if (typeof value !== 'number' && value !== null) {
      throw new TypeError(
        `Expected a number for SmallMoneyType, but got ${typeof value}`,
      );
    }
    return true;
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
    encoding?: Encoding,
  ): unknown {
    this.validate(value);
    return fromJsToNapiMoneyTransformer(value as number | null);
  }
}
