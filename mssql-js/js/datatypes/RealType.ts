import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import type { Encoding } from '../codepages';

export class RealType extends DataType {
  constructor() {
    super(JsSqlDataTypes.Real);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    return typeof value === 'number';
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
    encoding?: Encoding,
  ): unknown {
    if (value === null) return null;
    if (typeof value === 'number') {
      return { value: value };
    }
    throw new TypeError('Expected a number for Real types');
  }
}
