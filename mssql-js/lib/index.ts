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
  OutputParams,
  NapiIsolationLevel,
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
import {
  codepageByLanguageId,
  codepageBySortId,
  Encoding,
} from './codepages.js';

export { Request };
export { Transaction, IsolationLevel } from './transactions.js';

export type { JsClientContext, Connection, Metadata };

export function create_connection(
  context: JsClientContext,
): Promise<SqlJsConnection> {
  return connect(context).then(
    (internal_connection) => new SqlJsConnection(internal_connection),
  );
}

import { IsolationLevel } from './transactions.js';

export class SqlJsConnection {
  constructor(private internal_connection: Connection) {
    this.internal_connection = internal_connection;
  }

  /**
   * Begins a transaction with an optional isolation level and name.
   */
  async beginTransaction(
    isolationLevel: IsolationLevel = IsolationLevel.ReadCommitted,
    name?: string,
  ): Promise<void> {
    return this.internal_connection.beginTransaction(
      isolationLevel as unknown as NapiIsolationLevel,
      name,
    );
  }

  /**
   * Commits the current transaction.
   */
  async commitTransaction(): Promise<void> {
    return this.internal_connection.commitTransaction();
  }

  /**
   * Rolls back the current transaction or to a savepoint if a name is provided.
   */
  async rollbackTransaction(name?: string): Promise<void> {
    return this.internal_connection.rollbackTransaction(name);
  }

  /**
   * Creates a savepoint with the given name in the current transaction.
   */
  async saveTransaction(name: string): Promise<void> {
    return this.internal_connection.saveTransaction(name);
  }

  getEncoding(): Encoding {
    let db_collation = this.internal_connection.getCollation();
    let encoding: Encoding = 'utf-8';
    if (db_collation != null) {
      if (db_collation.isUtf8) {
        encoding = 'utf-8';
      } else if (db_collation.sortId !== 0) {
        encoding = codepageBySortId[db_collation.sortId];
      } else {
        encoding = codepageByLanguageId[db_collation.lcidLanguageId];
      }
    } else {
      encoding = 'utf-8';
    }
    return encoding;
  }

  async execute(query: string, params?: Array<Parameter>): Promise<void> {
    if (params && params.length > 0) {
      return this.internal_connection.executeWithParams(query, params);
    } else {
      return this.internal_connection.execute(query);
    }
  }

  async executeProc(
    storedProcName: string,
    namedParams: Array<Parameter>,
  ): Promise<void> {
    return this.internal_connection.executeProc(storedProcName, namedParams);
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

  async close(): Promise<void> {
    return this.internal_connection.close();
  }

  async closeQuery(): Promise<void> {
    return this.internal_connection.closeQuery();
  }

  async getReturnValues(): Promise<Array<OutputParams> | null> {
    return this.internal_connection.getReturnValues();
  }

  // Transforms the NAPI types to JS types based on the metadata.
  static transform(
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
