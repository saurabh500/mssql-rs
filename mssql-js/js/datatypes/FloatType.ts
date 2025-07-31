import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';

export class FloatType extends DataType {
  constructor() {
    super(JsSqlDataTypes.Float);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    return typeof value === 'number';
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
  ): unknown {
    if (value === null) return null;
    if (typeof value === 'number') {
      return { value: value };
    }
    throw new TypeError('Expected a number for Float/Real types');
  }
}
