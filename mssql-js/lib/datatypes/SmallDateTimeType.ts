import { DataType } from './DataType';
import { JsSqlDataTypes } from './enums';
import { fromJsToNapiSmallDateTimeTransformer } from '../transformers/datetime';

export class SmallDateTimeType extends DataType {
  constructor() {
    super(JsSqlDataTypes.SmallDateTime);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    if (value === null) {
      return true; // Allow null values
    }
    if (!(value instanceof Date)) {
      throw new TypeError(
        `Expected a Date for SmallDateTimeType, but got ${typeof value}`,
      );
    }
    return true;
  }

  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
  ): unknown {
    this.validate(value);
    return fromJsToNapiSmallDateTimeTransformer(value as Date | null);
  }
}
