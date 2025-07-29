// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import {
  JsClientContext,
  Connection,
  Metadata,
  NapiSqlDateTime,
  NapiSqlTime,
  NapiSqlDateTime2,
  NapiDecimalParts,
  NapiSqlDateTimeOffset,
  NapiSqlMoney,
  Parameter,
  NapiF64,
} from './generated/index.js';
import { connect } from './generated/index.js';

// --- Transformers ---
import { fromNapiToJsDecimalTransformer } from './transformers/decimal';
import {
  fromNapiToJsSmallDateTimeTransformer,
  fromNapiToJsDateTimeTransformer,
  fromNapiToJsDateTransformer,
  fromNapiToJsDatetime2Transformer,
  fromNapiToJsDateTimeOffsetTransformer,
  fromNapiToJsTimeTransformer,
} from './transformers/datetime';
import {
  nCharNVarCharTransformer,
  varCharTdsTransformer,
  varCharTransformer,
} from './transformers/string';
import { fromNapiToJsBinaryTransformer } from './transformers/binary';
import { fromNapiToJsBitTransformer } from './transformers/bit';
import {
  fromNapiToJsIntTransformer,
  fromNapiToJsBigintTransformer,
} from './transformers/int';
import { moneyTransformer, smallMoneyTransformer } from './transformers/money';
import { fromNapiToJsGuidTransformer } from './transformers/guid';
import { floatTransformer } from './transformers/float';

import { Request } from './request.js';
import { Encoding } from './codepages.js';

export { Request };

export type { JsClientContext, Connection, Metadata };

export function create_connection(
  context: JsClientContext,
): Promise<SqlJsConnection> {
  return connect(context).then(
    (internal_connection) => new SqlJsConnection(internal_connection),
  );
}

export class SqlJsConnection {
  constructor(private internal_connection: Connection) {
    this.internal_connection = internal_connection;
  }

  async execute(query: string, params?: Array<Parameter>): Promise<void> {
    if (params && params.length > 0) {
      return this.internal_connection.executeWithParams(query, params);
    } else {
      return this.internal_connection.execute(query);
    }
  }

  async nextResultSet() {
    return this.internal_connection.nextResultSet();
  }

  async nextRowInResultset() {
    return this.internal_connection.nextRowInResultset();
  }

  async getMetadata() {
    return this.internal_connection.getMetadata();
  }

  async nextRow(): Promise<Array<Row>> {
    let metadata = await this.internal_connection.getMetadata();

    if (!metadata) {
      return [];
    }
    let next_row = await this.internal_connection.nextRowInResultset();
    if (!next_row) {
      if (!(await this.internal_connection.nextResultSet())) {
        return [];
      } else {
        metadata = await this.internal_connection.getMetadata();
        if (!metadata) {
          return [];
        }
        next_row = await this.internal_connection.nextRowInResultset();
      }
    }
    let items: Array<Row> = [];
    if (next_row) {
      next_row.forEach((rowVal, index) => {
        let transformed = this.transform(metadata[index], rowVal);
        items.push(transformed);
      });
    }
    return items;
  }

  async close(): Promise<void> {
    return this.internal_connection.close();
  }

  async closeQuery(): Promise<void> {
    return this.internal_connection.closeQuery();
  }

  transform(
    metadata: Metadata,
    row:
      | number
      | bigint
      | boolean
      | Buffer
      | null
      | NapiSqlDateTime
      | NapiSqlTime
      | NapiSqlDateTime
      | NapiSqlDateTime2
      | NapiSqlDateTimeOffset
      | NapiSqlMoney
      | NapiDecimalParts
      | string
      | NapiF64,
  ): Row {
    let jsdatatype = metadata.dataType as unknown as JsSqlDataTypes;

    // Use transformer dictionary if available
    const transformer = tdsToJsTransformers[jsdatatype];
    if (transformer) {
      return {
        metadata,
        rowVal: transformer(metadata, row),
      };
    }

    switch (jsdatatype) {
      default:
        return {
          metadata,
          rowVal: null,
        };
    }
  }
}

export interface Row {
  metadata: Metadata;
  rowVal: number | string | boolean | Buffer | null | Date | bigint;
}

export interface TdsToJsTransformer {
  (metadata: Metadata, row: any): any;
}

export interface JsToTdsTransformer {
  (row: any, encoding?: Encoding): any;
}

export enum JsSqlDataTypes {
  Void = 31,
  Image = 34,
  Text = 35,
  UniqueIdentifier = 36,
  VarBinary = 37,
  VarChar = 39,
  Date = 40,
  Time = 41,
  DateTime2 = 42,
  DateTimeOffset = 43,
  Binary = 45,
  Char = 47,
  TinyInt = 48,
  Bit = 50,
  SmallInt = 52,
  Decimal = 55,
  Int = 56,
  SmallDateTime = 58,
  Real = 59,
  Money = 60,
  DateTime = 61,
  Float = 62,
  Numeric = 63,
  SsVariant = 98,
  NText = 99,
  FltN = 109,
  SmallMoney = 122,
  BigInt = 127,
  BigVarBinary = 165,
  BigVarChar = 167,
  BigBinary = 173,
  BigChar = 175,
  NVarChar = 231,
  NChar = 239,
  Udt = 240,
  Xml = 241,
  Json = 244,
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
  [JsSqlDataTypes.VarBinary]: fromNapiToJsBinaryTransformer,
  [JsSqlDataTypes.BigVarBinary]: fromNapiToJsBinaryTransformer,
  [JsSqlDataTypes.BigBinary]: fromNapiToJsBinaryTransformer,
  [JsSqlDataTypes.Binary]: fromNapiToJsBinaryTransformer,
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
