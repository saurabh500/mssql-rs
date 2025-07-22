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
} from './generated/index.js';
import { connect } from './generated/index.js';

// --- Transformers ---
import { decimalTransformer } from './transformers/decimal';
import {
  smallDateTimeTransformer,
  dateTimeTransformer,
  dateTransformer,
  dateTime2Transformer,
  dateTimeOffsetTransformer,
  timeTransformer,
} from './transformers/datetime';
import {
  nCharNVarCharTransformer,
  varCharTransformer,
} from './transformers/string';
import { binaryTransformer } from './transformers/binary';
import { bitTransformer } from './transformers/bit';
import { intTransformer, bigintTransformer } from './transformers/int';
import { moneyTransformer, smallMoneyTransformer } from './transformers/money';
import { guidTransformer } from './transformers/guid';
import { floatTransformer } from './transformers/float';

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
      | string,
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
  [JsSqlDataTypes.Decimal]: decimalTransformer,
  [JsSqlDataTypes.Numeric]: decimalTransformer,
  [JsSqlDataTypes.SmallDateTime]: smallDateTimeTransformer,
  [JsSqlDataTypes.DateTime]: dateTimeTransformer,
  [JsSqlDataTypes.Date]: dateTransformer,
  [JsSqlDataTypes.DateTime2]: dateTime2Transformer,
  [JsSqlDataTypes.DateTimeOffset]: dateTimeOffsetTransformer,
  [JsSqlDataTypes.Time]: timeTransformer,
  [JsSqlDataTypes.NChar]: nCharNVarCharTransformer,
  [JsSqlDataTypes.NVarChar]: nCharNVarCharTransformer,
  [JsSqlDataTypes.VarChar]: varCharTransformer,
  [JsSqlDataTypes.Char]: varCharTransformer,
  [JsSqlDataTypes.BigVarChar]: varCharTransformer,
  [JsSqlDataTypes.BigChar]: varCharTransformer,
  [JsSqlDataTypes.VarBinary]: binaryTransformer,
  [JsSqlDataTypes.BigVarBinary]: binaryTransformer,
  [JsSqlDataTypes.BigBinary]: binaryTransformer,
  [JsSqlDataTypes.Binary]: binaryTransformer,
  [JsSqlDataTypes.Image]: binaryTransformer,
  [JsSqlDataTypes.Bit]: bitTransformer,
  [JsSqlDataTypes.TinyInt]: intTransformer,
  [JsSqlDataTypes.SmallInt]: intTransformer,
  [JsSqlDataTypes.Int]: intTransformer,
  [JsSqlDataTypes.BigInt]: bigintTransformer,
  [JsSqlDataTypes.Money]: moneyTransformer,
  [JsSqlDataTypes.SmallMoney]: smallMoneyTransformer,
  [JsSqlDataTypes.UniqueIdentifier]: guidTransformer,
  [JsSqlDataTypes.Real]: floatTransformer,
  [JsSqlDataTypes.Float]: floatTransformer,
  [JsSqlDataTypes.FltN]: floatTransformer,
};
