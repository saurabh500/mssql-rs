import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import type { Encoding } from '../codepages';

export class BigIntType extends DataType {
  constructor() {
    super(JsSqlDataTypes.BigInt);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    return typeof value === 'number' && Number.isInteger(value);
  }
  transformForNapiWrites(
    row: number | string | Date | boolean | null,
    _encoding?: Encoding,
  ): unknown {
    if (row === null) return null;
    if (typeof row === 'bigint') return row;
    if (typeof row === 'number') return row;
    if (typeof row === 'string' && row.trim() !== '' && !isNaN(Number(row)))
      return Number(row);
    throw new TypeError(
      'Expected a non-empty string or number for TinyInt/SmallInt/Int/BigInt types',
    );
  }
}
