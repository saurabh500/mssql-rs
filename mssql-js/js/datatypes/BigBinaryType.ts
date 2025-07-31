import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';

export class BigBinaryType extends DataType {
  constructor() {
    super(JsSqlDataTypes.BigBinary);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    return value instanceof Uint8Array || value === null;
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
  ): unknown {
    return value;
  }
}
