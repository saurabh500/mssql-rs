// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';

import { Request } from '../dist/index.js';
import { TYPES } from '../dist/datatypes/types.js';
import { createContext, nextRow, openConnection } from './db.mjs';
import { JsSqlDataTypes } from '../dist/datatypes/enums.js';

test('connect to sqlserver and fetch multiple result sets', async (t) => {
  try {
    const connection = await openConnection(await createContext());
    t.pass('Connection successful');
    // Example of executing a query

    let query =
      'select top(1) * from sys.databases; select top(1) * from sys.tables; select top(1) * from sys.columns';
    await connection.execute(query);

    // select * from sys.databases
    let row = undefined;
    let row_count = 0;
    while (true) {
      row = await nextRow(connection);
      if (row && row.length > 0) {
        row_count++;
      } else {
        break;
      }
    }
    t.is(row_count, 3, 'Expected to fetch 3 rows');
    await connection.closeQuery();
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Connection should succeed');
  }
});

test('query using request.ts', async (t) => {
  try {
    const connection = await openConnection(await createContext());

    const request = new Request(connection);

    let result = await request.query('select 1, 2; select 10');

    //t.log('Received: ', result.IRecordSet);

    t.assert(result.rowCount === 2, 'Expected to fetch 2 rows');
    await connection.close();
    t.pass('Successfully queries using new Request class');
  } catch (err) {
    t.log(err);
    t.fail('Error querying');
  }
});

test('connect to sqlserver and execute parameterized query.', async (t) => {
  try {
    const connection = await openConnection(await createContext());
    t.pass('Connection successful');
    let query = 'select * from sys.columns where object_id > @input_parameter;';

    let params = [
      {
        name: '@input_parameter',
        dataType: JsSqlDataTypes.Int,
        value: 3,
        direction: 0,
      },
    ];

    await connection.execute(query, params);

    let row = undefined;
    let row_count = 0;
    while (true) {
      row = await nextRow(connection);
      if (row && row.length > 0) {
        row_count++;
      } else {
        break;
      }
    }
    t.assert(row_count > 1000, 'Expected to fetch more than 1000 rows');
    await connection.closeQuery();
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Connection should succeed');
  }
});

test('execute parameterized query with request class.', async (t) => {
  // Example TypeScript test with proper typing

  try {
    const connection = await openConnection(await createContext());
    t.pass('Connection successful');
    let query = 'select * from sys.columns where object_id > @input_parameter;';

    let request = new Request(connection);

    request.input('@input_parameter', TYPES.Int, 3);

    let result = await request.query(query);

    t.assert(result.rowCount > 1000, 'Expected to fetch more than 1000 rows');
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Connection should succeed');
  }
});

test('test strings with request class.', async (t) => {
  // Example TypeScript test with proper typing
  try {
    const connection = await openConnection(await createContext());
    t.pass('Connection successful');
    let query = 'select @str as string;';

    let request = new Request(connection);

    request.input('@str', TYPES.VarChar, 'test');

    let result = await request.query(query);

    t.assert(result.rowCount === 1, 'Expected to fetch exactly 1 row');
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Connection should succeed');
  }
});

test('adding @ to the parameter names if needed.', async (t) => {
  try {
    const connection = await openConnection(await createContext());
    t.pass('Connection successful');
    let query = 'select * from sys.columns where object_id > @input_parameter;';

    let request = new Request(connection);

    request.input('input_parameter', TYPES.Int, 3);

    let result = await request.query(query);

    t.assert(result.rowCount > 1000, 'Expected to fetch more than 1000 rows');
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Connection should succeed');
  }
});

test('decimal and numeric conversion', async (t) => {
  const connection = await openConnection(await createContext());
  // Test various decimal/numeric values, including large values that require multiple parts
  const testCases = [
    {
      sql: 'SELECT CAST(-0.00000000000000000000000000012345000456 AS DECIMAL(38,38)) AS val',
      expected: -0.00000000000000000000000000012345000456,
    },
    { sql: 'SELECT CAST(-123.45 AS DECIMAL(10,0)) AS val', expected: -123 },
    { sql: 'SELECT CAST(123.45 AS DECIMAL(10,2)) AS val', expected: 123.45 },
    {
      sql: 'SELECT CAST(-9876543210.12 AS DECIMAL(20,2)) AS val',
      expected: -9876543210.12,
    },
    {
      sql: 'SELECT CAST(4294967296 AS NUMERIC(20,0)) AS val',
      expected: 4294967296,
    }, // 0x100000000, triggers 2 parts
    {
      sql: 'SELECT CAST(18446744073709551616 AS NUMERIC(38,0)) AS val',
      expected: 18446744073709551616n,
    }, // 0x10000000000000000, triggers 3 parts, use BigInt
  ];
  for (const { sql, expected } of testCases) {
    await connection.execute(sql);
    const rows = await nextRow(connection);
    t.truthy(rows && rows.length > 0, `Should return a row for: ${sql}`);
    const val = rows[0].rowVal;
    if (typeof expected === 'bigint') {
      t.is(BigInt(val), expected, `Expected BigInt value for: ${sql}`);
    } else {
      t.is(Number(val), expected, `Expected numeric value for: ${sql}`);
    }
    await connection.closeQuery();
  }
  await connection.close();
});
