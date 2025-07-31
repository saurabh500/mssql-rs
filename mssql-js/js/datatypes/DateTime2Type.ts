import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import type { Encoding } from '../codepages';
import { fromJsToNapiDatetime2Transformer } from '../transformers/datetime';

export class DateTime2Type extends DataType {
  constructor(public scale: number) {
    super(JsSqlDataTypes.DateTime2);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    if (value === null) {
      return true; // Allow null values
    }
    if (!(value instanceof Date)) {
      throw new TypeError(
        `Expected a Date for Dateime2Type, but got ${typeof value}`,
      );
    }
    return true;
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
    _encoding?: Encoding,
  ): unknown {
    this.validate(value);
    return fromJsToNapiDatetime2Transformer(value as Date | null);
  }
}
