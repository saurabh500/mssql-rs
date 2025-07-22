// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { JsSqlDataTypes, SqlJsConnection, Row } from '.';

interface IResult {
  IRecordSet: Row[][];
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
      IRecordSet: [],
      row_count: 0,
    };

    while (true) {
      let currentResultSet: Row[] = [];

      // Process all rows in the current result set
      while (true) {
        let row = await this.connection.nextRow();
        if (row && row.length > 0) {
          currentResultSet.push(...row);
          result.row_count++;
        } else {
          // No more rows in current result set
          break;
        }
      }

      // Add the current result set to the results (even if empty)
      if (currentResultSet.length > 0) {
        result.IRecordSet.push(currentResultSet);
      }

      // Try to move to the next result set
      const hasNextResultSet = await this.connection.nextResultSet();
      if (!hasNextResultSet) {
        // No more result sets
        break;
      }
    }

    await this.connection.closeQuery();

    return result;
  }
}
