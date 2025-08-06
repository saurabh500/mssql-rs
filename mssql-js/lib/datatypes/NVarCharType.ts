import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import type { Encoding } from '../codepages';
import { nCharNVarCharTdsTransformer } from '../transformers/string';

export class NVarCharType extends DataType {
  private _length: number;
  static readonly maxLength = 4000;
  constructor(length: number) {
    super(JsSqlDataTypes.NVarChar);
    if (
      !((length >= 1 && length <= NVarCharType.maxLength) || length === -1) &&
      length !== undefined
    ) {
      throw new RangeError(
        `NVarCharType length must be between 1 and ${NVarCharType.maxLength}, or -1 for MAX. Received: ${length}`,
      );
    }
    this._length = length;
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
    if (this._length === undefined || this._length === null) {
      this._length = value === null ? 0 : (value as string).length;
    }
    return nCharNVarCharTdsTransformer(value as string | null, encoding);
  }

  length(): number {
    // We force the mssql-tds to change to NVARCHAR(MAX) by providing a length > 4000
    if (
      this._length === undefined ||
      this._length === null ||
      this._length < 0
    ) {
      this._length = NVarCharType.maxLength + 1;
    }
    return this._length;
  }
}
