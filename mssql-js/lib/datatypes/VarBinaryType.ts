import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';

export class VarBinaryType extends DataType {
  private _length: number;
  static readonly maxLength = 8000;

  constructor(length: number) {
    super(JsSqlDataTypes.VarBinary);
    if (!(length >= 1 && length <= VarBinaryType.maxLength) || length === -1) {
      throw new RangeError(
        `VarBinaryType length must be between 1 and ${VarBinaryType.maxLength}, or it should be -1 for MAX. Received: ${length}`,
      );
    }
    this._length = length;
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    if (value === null) return true; // Allow null values
    if (Buffer.isBuffer(value)) return true;
    throw new TypeError(
      `Expected a Buffer for VarBinary, but got ${typeof value}`,
    );
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
  ): unknown {
    this.validate(value);
    if (value === null) return null; // Handle null case
    if (Buffer.isBuffer(value)) return value; // Return the Buffer directly
    throw new TypeError(
      `Expected a Buffer for VarBinary, but got ${typeof value}`,
    );
  }

  length(): number {
    // We force the mssql-tds to change to NVARCHAR(MAX) by providing a length > 4000
    if (
      this._length === undefined ||
      this._length === null ||
      this._length < 0
    ) {
      this._length = VarBinaryType.maxLength + 1;
    }
    return this._length;
  }
}
