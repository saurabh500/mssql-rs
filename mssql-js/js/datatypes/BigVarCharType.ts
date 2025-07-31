import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import type { Encoding } from '../codepages';

export class BigVarCharType extends DataType {
  constructor() {
    super(JsSqlDataTypes.BigVarChar);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    return typeof value === 'string';
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
    encoding?: Encoding,
  ): unknown {
    return value;
  }
}
