import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import type { Encoding } from '../codepages';
import { fromJsToSmallMoneyTransformer } from '../transformers/money';

export class SmallMoneyType extends DataType {
  constructor() {
    super(JsSqlDataTypes.SmallMoney);
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
    _encoding?: Encoding,
  ): unknown {
    this.validate(value);
    return fromJsToSmallMoneyTransformer(value as number | null);
  }
}
