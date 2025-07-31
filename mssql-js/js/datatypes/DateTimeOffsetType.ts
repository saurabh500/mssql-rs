import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import type { Encoding } from '../codepages';
import { fromJsToNapiDateTimeOffsetTransformer } from '../transformers/datetime';

export class DateTimeOffsetType extends DataType {
  constructor(public scale: number) {
    super(JsSqlDataTypes.DateTimeOffset);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    if (value === null) {
      return true; // Allow null values
    }
    if (!(value instanceof Date)) {
      throw new TypeError(
        `Expected a Date for DateTimeOffset, but got ${typeof value}`,
      );
    }
    return true;
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
    _encoding?: Encoding,
  ): unknown {
    this.validate(value);
    return fromJsToNapiDateTimeOffsetTransformer(value as Date | null);
  }
}
