import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';

export class BigCharType extends DataType {
  constructor() {
    super(JsSqlDataTypes.BigChar);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    return typeof value === 'string';
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
  ): unknown {
    return value;
  }
}
