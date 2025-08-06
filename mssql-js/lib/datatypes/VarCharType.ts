import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import { varCharTdsTransformer } from '../transformers/string';
import { Encoding } from '../codepages';

export class VarCharType extends DataType {
  private _length: number;
  static readonly maxLength = 8000;

  constructor(length: number) {
    super(JsSqlDataTypes.VarChar);
    if (
      length !== undefined &&
      (!(length >= 1 && length <= VarCharType.maxLength) || length === -1)
    ) {
      throw new RangeError(
        `VarCharType length must be between 1 and ${VarCharType.maxLength}, or it should be -1 for MAX. Received: ${length}`,
      );
    }
    this._length = length;
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

  length(): number {
    // We force the mssql-tds to change to NVARCHAR(MAX) by providing a length > 4000
    if (
      this._length === undefined ||
      this._length === null ||
      this._length < 0
    ) {
      this._length = VarCharType.maxLength + 1;
    }
    return this._length;
  }
}
