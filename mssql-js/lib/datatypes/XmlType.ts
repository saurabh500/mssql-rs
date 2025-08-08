import { DataType } from './DataType';
import { JsSqlDataTypes } from './enums';

export class XmlType extends DataType {
  constructor() {
    super(JsSqlDataTypes.Xml);
  }
  validate(value: bigint | number | string | Date | boolean | null): boolean {
    return typeof value === 'string';
  }
  transformForNapiWrites(
    _value: bigint | number | string | Date | boolean | null,
  ): unknown {
    throw new Error('not implemented');
  }
}
