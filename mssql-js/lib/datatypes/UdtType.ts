import { DataType } from './DataType';
import { JsSqlDataTypes } from './enums';

export class UdtType extends DataType {
  constructor() {
    super(JsSqlDataTypes.Udt);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    return true;
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
  ): unknown {
    return value;
  }
}
