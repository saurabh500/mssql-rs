import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import type { Encoding } from '../codepages';

export class IntType extends DataType {
  constructor() {
    super(JsSqlDataTypes.Int);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    return typeof value === 'number' && Number.isInteger(value);
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
    _encoding?: Encoding,
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
