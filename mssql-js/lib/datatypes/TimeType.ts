import { DataType } from './DataType';
import { JsSqlDataTypes } from './enums';
import { fromJsToNapiTimeTransformer } from '../transformers/datetime';

export class TimeType extends DataType {
  constructor(public scale: number) {
    super(JsSqlDataTypes.Time);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    if (value === null) {
      return true; // Allow null values
    }
    if (!(value instanceof Date)) {
      throw new TypeError(
        `Expected a Date for TimeType, but got ${typeof value}`,
      );
    }
    return true;
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
  ): unknown {
    this.validate(value);
    return fromJsToNapiTimeTransformer(value as Date | null, this.scale);
  }
}
