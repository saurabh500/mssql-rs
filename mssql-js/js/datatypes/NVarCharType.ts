import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import type { Encoding } from '../codepages';
import { nCharNVarCharTdsTransformer } from '../transformers/string';

export class NVarCharType extends DataType {
  constructor() {
    super(JsSqlDataTypes.NVarChar);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    if (value === null) {
      return true; // Allow null values
    }
    if (typeof value !== 'string') {
      throw new TypeError(
        `Expected a string for NVarCharType, but got ${typeof value}`,
      );
    }
    return true;
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
    encoding?: Encoding,
  ): unknown {
    this.validate(value);
    return nCharNVarCharTdsTransformer(value as string | null, encoding);
  }
}
