// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
import { BigVarCharType } from './BigVarCharType';
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

// --- Transformers ---
import { fromNapiToJsDecimalTransformer } from '../transformers/decimal';
import {
  fromNapiToJsSmallDateTimeTransformer,
  fromNapiToJsDateTimeTransformer,
  fromNapiToJsDateTransformer,
  fromNapiToJsDatetime2Transformer,
  fromNapiToJsDateTimeOffsetTransformer,
  fromNapiToJsTimeTransformer,
} from '../transformers/datetime';
import {
  nCharNVarCharTransformer,
  varCharTdsTransformer,
  varCharTransformer,
} from '../transformers/string';
import { fromNapiToJsBinaryTransformer } from '../transformers/binary';
import { fromNapiToJsBitTransformer } from '../transformers/bit';
import {
  fromNapiToJsIntTransformer,
  fromNapiToJsBigintTransformer,
} from '../transformers/int';
import { moneyTransformer, smallMoneyTransformer } from '../transformers/money';
import { fromNapiToJsGuidTransformer } from '../transformers/guid';
import { floatTransformer } from '../transformers/float';
import { Encoding } from '../codepages.js';
import { Metadata } from '../generated';
import { JsSqlDataTypes } from './enums';

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

  BigVarChar: new BigVarCharType(),

  BigChar: new BigCharType(),
  Udt: new UdtType(),
  Xml: new XmlType(),
  Json: new JsonType(),

  NVarChar(length: number): NVarCharType {
    return new NVarCharType(length);
  },

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

export interface TdsToJsTransformer {
  (metadata: Metadata, row: any): any;
}

export interface JsToTdsTransformer {
  (row: any, encoding?: Encoding): any;
}

export const tdsToJsTransformers: Partial<
  Record<JsSqlDataTypes, TdsToJsTransformer>
> = {
  [JsSqlDataTypes.Decimal]: fromNapiToJsDecimalTransformer,
  [JsSqlDataTypes.Numeric]: fromNapiToJsDecimalTransformer,
  [JsSqlDataTypes.SmallDateTime]: fromNapiToJsSmallDateTimeTransformer,
  [JsSqlDataTypes.DateTime]: fromNapiToJsDateTimeTransformer,
  [JsSqlDataTypes.Date]: fromNapiToJsDateTransformer,
  [JsSqlDataTypes.DateTime2]: fromNapiToJsDatetime2Transformer,
  [JsSqlDataTypes.DateTimeOffset]: fromNapiToJsDateTimeOffsetTransformer,
  [JsSqlDataTypes.Time]: fromNapiToJsTimeTransformer,
  [JsSqlDataTypes.NChar]: nCharNVarCharTransformer,
  [JsSqlDataTypes.NVarChar]: nCharNVarCharTransformer,
  [JsSqlDataTypes.VarChar]: varCharTransformer,
  [JsSqlDataTypes.Char]: varCharTransformer,
  [JsSqlDataTypes.BigVarChar]: varCharTransformer,
  [JsSqlDataTypes.BigChar]: varCharTransformer,
  [JsSqlDataTypes.BigVarBinary]: fromNapiToJsBinaryTransformer,
  [JsSqlDataTypes.BigBinary]: fromNapiToJsBinaryTransformer,
  [JsSqlDataTypes.Image]: fromNapiToJsBinaryTransformer,
  [JsSqlDataTypes.Bit]: fromNapiToJsBitTransformer,
  [JsSqlDataTypes.TinyInt]: fromNapiToJsIntTransformer,
  [JsSqlDataTypes.SmallInt]: fromNapiToJsIntTransformer,
  [JsSqlDataTypes.Int]: fromNapiToJsIntTransformer,
  [JsSqlDataTypes.BigInt]: fromNapiToJsBigintTransformer,
  [JsSqlDataTypes.Money]: moneyTransformer,
  [JsSqlDataTypes.SmallMoney]: smallMoneyTransformer,
  [JsSqlDataTypes.UniqueIdentifier]: fromNapiToJsGuidTransformer,
  [JsSqlDataTypes.Real]: floatTransformer,
  [JsSqlDataTypes.Float]: floatTransformer,
  [JsSqlDataTypes.FltN]: floatTransformer,
};

export const jsToTdstransformers: Partial<
  Record<JsSqlDataTypes, JsToTdsTransformer>
> = {
  [JsSqlDataTypes.VarChar]: varCharTdsTransformer,
};
