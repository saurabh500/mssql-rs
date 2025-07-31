import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import { varCharTdsTransformer } from '../transformers/string';
import { Encoding } from '../codepages';

export class VarCharType extends DataType {
  constructor(public length: number) {
    super(JsSqlDataTypes.VarChar);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    if (typeof value != 'string' && value != null) {
      throw new TypeError(
        `Expected a string for VarCharType, but got ${typeof value}`,
      );
    }
    return true;
  }

  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
    encoding?: Encoding,
  ): unknown {
    return varCharTdsTransformer(value as string | null, encoding);
  }
}
