import { ImageType } from './ImageType';
import { TextType } from './TextType';
import { UniqueIdentifierType } from './UniqueIdentifierType';
import { DateType } from './DateType';
import { TinyIntType } from './TinyIntType';
import { BitType } from './BitType';
import { SmallIntType } from './SmallIntType';
import { IntType } from './IntType';
import { SmallDateTimeType } from './SmallDateTimeType';
import { RealType } from './RealType';
import { MoneyType } from './MoneyType';
import { DateTimeType } from './DateTimeType';
import { FloatType } from './FloatType';
import { NTextType } from './NTextType';
import { SmallMoneyType } from './SmallMoneyType';
import { BigIntType } from './BigIntType';
import { BigVarBinaryType } from './BigVarBinaryType';
import { BigVarCharType } from './BigVarCharType';
import { BigBinaryType } from './BigBinaryType';
import { BigCharType } from './BigCharType';
import { NVarCharType } from './NVarCharType';
import { UdtType } from './UdtType';
import { XmlType } from './XmlType';
import { JsonType } from './JsonType';
import { VarBinaryType } from './VarBinaryType';
import { VarCharType } from './VarCharType';
import { TimeType } from './TimeType';
import { DateTime2Type } from './DateTime2Type';
import { DateTimeOffsetType } from './DateTimeOffsetType';
import { BinaryType } from './BinaryType';
import { CharType } from './CharType';
import { DecimalType } from './DecimalType';
import { NumericType } from './NumericType';
import { NCharType } from './NCharType';

export const TYPES = {
  Image: new ImageType(),
  Text: new TextType(),
  UniqueIdentifier: new UniqueIdentifierType(),
  Date: new DateType(),
  TinyInt: new TinyIntType(),
  Bit: new BitType(),
  SmallInt: new SmallIntType(),
  Int: new IntType(),
  SmallDateTime: new SmallDateTimeType(),
  Real: new RealType(),
  Money: new MoneyType(),
  DateTime: new DateTimeType(),
  Float: new FloatType(),
  NText: new NTextType(),
  SmallMoney: new SmallMoneyType(),
  BigInt: new BigIntType(),
  BigVarBinary: new BigVarBinaryType(),
  BigVarChar: new BigVarCharType(),
  BigBinary: new BigBinaryType(),
  BigChar: new BigCharType(),
  NVarChar: new NVarCharType(),
  Udt: new UdtType(),
  Xml: new XmlType(),
  Json: new JsonType(),

  // Parameterized types
  VarBinary(length: number): VarBinaryType {
    return new VarBinaryType(length);
  },
  VarChar(length: number): VarCharType {
    return new VarCharType(length);
  },
  Time(scale: number): TimeType {
    return new TimeType(scale);
  },
  DateTime2(scale: number): DateTime2Type {
    return new DateTime2Type(scale);
  },
  DateTimeOffset(scale: number): DateTimeOffsetType {
    return new DateTimeOffsetType(scale);
  },
  Binary(length: number): BinaryType {
    return new BinaryType(length);
  },
  Char(length: number): CharType {
    return new CharType(length);
  },
  Decimal(precision: number, scale: number): DecimalType {
    return new DecimalType(precision, scale);
  },
  Numeric(precision: number, scale: number): NumericType {
    return new NumericType(precision, scale);
  },
  NChar(length: number): NCharType {
    return new NCharType(length);
  },
};
