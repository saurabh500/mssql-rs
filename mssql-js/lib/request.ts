// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { SqlJsConnection } from '.';
import { DataType } from './datatypes';
import { JsSqlDataTypes } from './datatypes/enums';
import { SqlDataTypes, Parameter } from './generated';

type ColumnValue = number | string | boolean | Buffer | null | Date | bigint;

// makes a row interface that assigns a key string to a value
export interface RecordSetRow {
  [key: string]: ColumnValue | Array<ColumnValue>;
}

// interface to group together column metadata
export interface Column {
  index: number;
  name: string;
  type: SqlDataTypes | undefined;
}

// creates an array of record set rows with columns and row count properties
export interface RecordSet extends Array<RecordSetRow> {
  columns: Column[];
  rowCount: number;
}

export interface IResult {
  IRecordSets: RecordSet[];
  IRecordSet: RecordSet | null;
  rowCount: number;
  output: {
    [key: string]: number | string | boolean | Buffer | null | Date | bigint;
  };
}

//data types that are able to be parameterized
export type JsSqlParameterTypes =
  | JsSqlDataTypes.VarChar
  | JsSqlDataTypes.Date
  | JsSqlDataTypes.Time
  | JsSqlDataTypes.DateTime2
  | JsSqlDataTypes.DateTimeOffset
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

  input(varName: string, type: DataType | (() => DataType), value: unknown) {
    //adds a '@' to a variable name if the use does not put one
    if (!varName.startsWith('@')) {
      varName = '@' + varName;
    }
    //if the type is a function, call it to get the DataType
    if (typeof type === 'function') {
      type = type();
    }

    let sqltype: JsSqlParameterTypes;
    if (typeof type === 'object' && 'sqlType' in type) {
      sqltype = type.sqlType as JsSqlParameterTypes;
      let transformed_value = type.transformForNapiWrites(
        value as unknown as number | string | Date | boolean | null,
        this.connection.getEncoding(),
      );

      //collects the input parameters into the global parameters
      this.params.push({
        name: varName,
        dataType: sqltype as unknown as SqlDataTypes,
        value: transformed_value,
        direction: 0,
        length: typeof type.length === 'function' ? type.length() : undefined,
      });
    } else {
      throw new TypeError('Invalid type provided for input');
    }
  }

  output(varName: string, type: DataType | (() => DataType), value: unknown) {
    //adds a '@' to a variable name if the use does not put one
    if (!varName.startsWith('@')) {
      varName = '@' + varName;
    }
    //if the type is a function, call it to get the DataType
    if (typeof type === 'function') {
      type = type();
    }

    let sqltype: JsSqlParameterTypes;
    if (typeof type === 'object' && 'sqlType' in type) {
      sqltype = type.sqlType as JsSqlParameterTypes;
      if (value === undefined) {
        value = null; // default to null if no value is provided
      }
      let transformed_value = type.transformForNapiWrites(
        value as unknown as number | string | Date | boolean | null,
        this.connection.getEncoding(),
      );
      //collects the input parameters into the global parameters
      this.params.push({
        name: varName,
        dataType: sqltype as unknown as SqlDataTypes,
        value: transformed_value,
        direction: 1,
      });
    } else {
      throw new TypeError(
        'Invalid type provided for output. Expected a DataType object.',
      );
    }
  }

  async query(command: string): Promise<IResult> {
    //will correctly run regardless if there are parameters or not
    await this.connection.execute(command, this.params);
    let result: IResult = await this.createResult();

    await this.connection.closeQuery();

    return result;
  }

  async execute(storedProcName: string): Promise<IResult> {
    //will correctly run regardless if there are parameters or not
    await this.connection.executeProc(storedProcName, this.params);

    let result: IResult = await this.createResult();

    let returnValues = await this.connection.getReturnValues();
    if (returnValues) {
      result.output = {};
      returnValues.forEach((rv) => {
        let paramName = rv.name;
        let paramValue = rv.value;
        if (paramName.charAt(0) === '@') {
          paramName = paramName.slice(1);
        }

        let transformedVal = SqlJsConnection.transform(rv.metadata, paramValue);
        result.output[paramName] = transformedVal.rowVal;
      });
    }
    await this.connection.closeQuery();

    return result;
  }

  // Create the result object by processing all rows from the executed commands.
  // This iterates over all the result sets and creates the recordsets.
  private async createResult() {
    let result: IResult = {
      IRecordSets: [],
      IRecordSet: null,
      rowCount: 0,
      output: {},
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
        let currentRow: RecordSetRow = {};
        let anonymousColumns: number = 0;
        next_row.forEach((rowVal, index) => {
          if (index >= metadata.length) {
            throw new Error(`Index ${index} out of bounds for metadata array`);
          }
          let transformed = SqlJsConnection.transform(metadata[index], rowVal);
          //avoids adding the same columns
          if (currentRecordSet.columns.length + anonymousColumns <= index) {
            let column = {
              index: index,
              name: transformed.metadata.name,
              //if the column is an anonymous column, make the column type undefined
              type:
                transformed.metadata.name.length > 0
                  ? transformed.metadata.dataType
                  : undefined,
            };

            //avoids readding unnamed columns for each anonymous column
            if (!(column.name === '' && '' in currentRow)) {
              currentRecordSet.columns.push(column);
            }
          }
          //pushes the row item as a column name and associated value
          //if there is no name associated such as if querying "SELECT 10", add it to an array for all anonymous columns
          if (transformed.metadata.name === '' && '' in currentRow) {
            if (Array.isArray(currentRow[''])) {
              currentRow[''].push(transformed.rowVal);
              anonymousColumns++; //only increments the count for anonymous columns after the first
            }

            //creates an array only if there are more than 1 anonymous column values
            else {
              currentRow[''] = [currentRow[''], transformed.rowVal];
            }
          } else {
            currentRow[transformed.metadata.name] = transformed.rowVal;
          }
        });
        //keeps track of the row count for each record set
        currentRecordSet.push(currentRow);
        currentRecordSet.rowCount++;
      }

      //keeps count of every rows in all record sets
      result.rowCount += currentRecordSet.rowCount;
      result.IRecordSets.push(currentRecordSet);

      if (!(await this.connection.nextResultSet())) {
        break;
      }
    }

    result.IRecordSet =
      result.IRecordSets.length > 0 ? result.IRecordSets[0] : null;
    return result;
  }
}
