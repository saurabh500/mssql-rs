// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { JsSqlDataTypes, SqlJsConnection } from '.';
import { Encoding } from './codepages';
import { SqlDataTypes, Parameter } from './generated';
import {
  nCharNVarCharTdsTransformer,
  varCharTdsTransformer,
} from './transformers/string';

type ColumnValue =
  | number
  | string
  | boolean
  | Buffer
  | null
  | Date
  | bigint
  | Array<Column>;

// makes a row interface that assigns a key string to a value
interface RecordSetRow {
  [key: string]: ColumnValue;
}

// interface to group together column metadata
interface Column {
  index: number;
  name: string;
  type: SqlDataTypes | undefined;
}

// creates an array of record set rows with columns and row count properties
interface RecordSet extends Array<RecordSetRow> {
  columns: Column[];
  rowCount: number;
}

interface IResult {
  IRecordSets: RecordSet[];
  IRecordSet: RecordSet | null;
  rowCount: number;
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
    //adds a '@' to a variable name if the use does not put one
    if (!varName.startsWith('@')) {
      varName = '@' + varName;
    }

    // Transform from Js Types to Tds Types.
    let transformed_value = transformForWrites(type, value);

    //collects the inputed parameters into the global parameters
    this.params.push({
      name: varName,
      dataType: type as unknown as SqlDataTypes,
      value: transformed_value,
    });
  }

  async query(command: string): Promise<IResult> {
    //will correctly run regardless if there are parameters or not
    await this.connection.execute(command, this.params);

    let result: IResult = {
      IRecordSets: [],
      IRecordSet: null,
      rowCount: 0,
    };

    // Process all rows from the executed commands
    while (true) {
      //gets the metadata for the current result set
      let currentRecordSet: RecordSet = Object.assign([], {
        columns: [],
        rowCount: 0,
      });
      let metadata = await this.connection.getMetadata();
      if (!metadata || metadata.length === 0) {
        break;
      }
      //build the current result set
      while (true) {
        let next_row = await this.connection.nextRowInResultset();
        if (!next_row) {
          break;
        }
        //builds the current row as an object and counts the number of anonymous columns
        let currentRow = {};
        let anonymousColumns: number = 0;
        next_row.forEach((rowVal, index) => {
          if (index >= metadata.length) {
            throw new Error(`Index ${index} out of bounds for metadata array`);
          }
          let transformed = this.connection.transform(metadata[index], rowVal);
          //avoids adding the same columns
          if (currentRecordSet.columns.length + anonymousColumns <= index) {
            let column = {
              index: index,
              name: transformed.metadata.name,
              type:
                //if the column is an anonymous column, make the type undefined
                transformed.metadata.name.length > 0
                  ? transformed.metadata.dataType
                  : undefined,
            };
            currentRecordSet.columns.push(column);
          }
          //pushes the row item as a column name and associated value
          //if there is no name associated such as if querying "SELECT 10", add it to an array for all anonymous columns
          if (transformed.metadata.name === '' && '' in currentRow) {
            if (Array.isArray(currentRow[''])) {
              currentRow[''].push(transformed.rowVal);
              anonymousColumns++; //only increments the count for anonymous columns past 1
            }
            //creates an array only if there are more than 1 anonymous column values
            else {
              currentRow[''] = [currentRow[''], transformed.rowVal];
            }
          } else {
            Object.assign(currentRow, {
              [transformed.metadata.name]: transformed.rowVal,
            });
          }
        });
        //keeps track of the row count for each record set
        currentRecordSet.push(currentRow);
        currentRecordSet.rowCount++;
      }

      //keeps count of all rows in all record sets
      result.rowCount += currentRecordSet.rowCount;
      result.IRecordSets.push(currentRecordSet);

      if (!(await this.connection.nextResultSet())) {
        break;
      }
    }

    result.IRecordSet =
      result.IRecordSets.length > 0 ? result.IRecordSets[0] : null;
    await this.connection.closeQuery();

    return result;
  }
}
function transformForWrites(
  type: JsSqlDataTypes,
  row: unknown,
  encoding?: Encoding,
) {
  switch (type) {
    case JsSqlDataTypes.VarBinary:
    case JsSqlDataTypes.Binary:
      if (Buffer.isBuffer(row)) return row;
      throw new TypeError('Expected a Buffer for VarBinary/Binary types');

    case JsSqlDataTypes.NVarChar:
    case JsSqlDataTypes.NChar:
      if (row === null || typeof row === 'string') {
        return nCharNVarCharTdsTransformer(row as string, encoding);
      } else {
        throw new TypeError('Expected a string for NVarChar/NChar');
      }
    case JsSqlDataTypes.Xml:
      throw new Error('not implemented');
    case JsSqlDataTypes.VarChar:
    case JsSqlDataTypes.Char:
      // check if the row type is string
      if (row === null || typeof row === 'string') {
        return varCharTdsTransformer(row, encoding);
      } else {
        throw new TypeError('Expected a string for VarChar/Char types');
      }
    case JsSqlDataTypes.Date:
    case JsSqlDataTypes.DateTime:
    case JsSqlDataTypes.DateTime2:
    case JsSqlDataTypes.SmallDateTime:
    case JsSqlDataTypes.DateTimeOffset:
      throw new Error('not implemented');
    case JsSqlDataTypes.Time:
      throw new Error('not implemented');
    case JsSqlDataTypes.TinyInt:
    case JsSqlDataTypes.SmallInt:
    case JsSqlDataTypes.Int:
    case JsSqlDataTypes.BigInt:
      if (typeof row === 'bigint') return row;
      if (typeof row === 'number') return row;
      if (typeof row === 'string' && row.trim() !== '' && !isNaN(Number(row)))
        return Number(row);
      throw new TypeError(
        'Expected a non-empty string or number for TinyInt/SmallInt/Int/BigInt types',
      );
    case JsSqlDataTypes.Bit:
      if (typeof row === 'boolean') return row;
      if (typeof row === 'number') return Boolean(row);
      if (typeof row === 'string') return row === 'true' || row === '1';
      return null;
    case JsSqlDataTypes.Decimal:
    case JsSqlDataTypes.Numeric:
    case JsSqlDataTypes.Money:
    case JsSqlDataTypes.SmallMoney:
    case JsSqlDataTypes.Float:
    case JsSqlDataTypes.Real:
      throw new Error('not implemented');
    case JsSqlDataTypes.UniqueIdentifier:
      throw new Error('not implemented');
    default:
      return row;
  }
}
