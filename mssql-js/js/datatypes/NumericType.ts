import { DataType } from './DataType';
import { JsSqlDataTypes } from '../.';
import { fromJsToNapiDecimalPartTransformer } from '../transformers/decimal';

export class NumericType extends DataType {
  constructor(
    public precision: number,
    public scale: number,
  ) {
    super(JsSqlDataTypes.Numeric);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    if (value === null) {
      return true; // Allow null values
    }
    if (typeof value !== 'number') {
      throw new TypeError(
        `Expected a number for DecimalType, but got ${typeof value}`,
      );
    }
    if (!Number.isFinite(value)) {
      throw new RangeError('DecimalType value must be a finite number');
    }
    if (this.precision < 1 || this.precision > 38) {
      throw new RangeError('Precision must be between 1 and 38');
    }
    if (this.scale < 0 || this.scale > this.precision) {
      throw new RangeError('Scale must be between 0 and precision');
    }
    return true;
  }
  transformForNapiWrites(
    value: bigint | number | string | Date | boolean | null,
  ): unknown {
    this.validate(value);
    return fromJsToNapiDecimalPartTransformer(
      value as number | null,
      this.scale,
      this.precision,
    );
  }
}
