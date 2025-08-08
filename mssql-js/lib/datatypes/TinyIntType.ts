import { DataType } from './DataType';
import { JsSqlDataTypes } from './enums';

export class TinyIntType extends DataType {
  constructor() {
    super(JsSqlDataTypes.TinyInt);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    return (
      typeof value === 'number' &&
      value >= 0 &&
      value <= 255 &&
      Number.isInteger(value)
    );
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
  ): unknown {
    if (value === null) return null;
    if (typeof value === 'bigint') return value;
    if (typeof value === 'number') return value;
    if (
      typeof value === 'string' &&
      value.trim() !== '' &&
      !isNaN(Number(value))
    )
      return Number(value);
    throw new TypeError(
      'Expected a non-empty string or number for TinyInt/SmallInt/Int/BigInt types',
    );
  }
}
