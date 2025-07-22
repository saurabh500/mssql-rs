// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { JsSqlDataTypes, SqlJsConnection, Row } from '.';

interface IResult {
  IRecordSets: Row[][];
  IRecordSet: Row[];
  row_count: number;
}

interface Parameter {
  name: string;
  type: JsSqlParameterTypes;
  value: unknown;
}

//data types that are able to be parameterized
export type JsSqlParameterTypes =
  | JsSqlDataTypes.VarBinary
  | JsSqlDataTypes.VarChar
  | JsSqlDataTypes.Date
  | JsSqlDataTypes.Time
  | JsSqlDataTypes.DateTime2
  | JsSqlDataTypes.DateTimeOffset
  | JsSqlDataTypes.Binary
  | JsSqlDataTypes.Char
  | JsSqlDataTypes.TinyInt
  | JsSqlDataTypes.Bit
  | JsSqlDataTypes.SmallInt
  | JsSqlDataTypes.Decimal
  | JsSqlDataTypes.Int
  | JsSqlDataTypes.SmallDateTime
  | JsSqlDataTypes.Real
  | JsSqlDataTypes.Money
  | JsSqlDataTypes.DateTime
  | JsSqlDataTypes.Float
  | JsSqlDataTypes.Numeric
  | JsSqlDataTypes.SmallMoney
  | JsSqlDataTypes.BigInt
  | JsSqlDataTypes.NVarChar
  | JsSqlDataTypes.NChar
  | JsSqlDataTypes.Xml
  | JsSqlDataTypes.UniqueIdentifier;

export class Request {
  connection: SqlJsConnection;
  private params: Parameter[];
  constructor(connection: SqlJsConnection) {
    this.connection = connection;
    this.params = [];
  }

  input(varName: string, type: JsSqlParameterTypes, value: unknown) {
    this.params.push({
      name: varName,
      type: type,
      value: value,
    });
  }

  async query(command: string): Promise<IResult> {
    await this.connection.execute(command);
    let result: IResult = {
      IRecordSets: [],
      IRecordSet: [],
      row_count: 0,
    };

    // Process all rows from the executed commands
    while (true) {
      let row = await this.connection.nextRow();
      if (row && row.length > 0) {
        result.IRecordSets.push(row);
        result.row_count++;
      } else {
        break;
      }
    }

    result.IRecordSet =
      result.IRecordSets.length > 0 ? result.IRecordSets[0] : [];
    await this.connection.closeQuery();

    return result;
  }
}
