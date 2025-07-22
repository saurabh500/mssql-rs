// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';

import { create_connection, JsSqlDataTypes } from '../js/index.js';
import { Request } from '../js/request.js';

test('connect to sqlserver and fetch multiple result sets', async (t) => {
  // Example TypeScript test with proper typing
  const context = {
    serverName: process.env.DB_HOST || 'localhost',
    port: 1433,
    userName: process.env.DB_USER || 'sa',
    password: process.env.SQL_PASSWORD,
    database: 'master',
    trustServerCertificate: true,
  };

  try {
    const connection = await create_connection(context);
    t.pass('Connection successful');
    // Example of executing a query

    let query =
      'select top(1) * from sys.databases; select top(1) * from sys.tables; select top(1) * from sys.columns';
    await connection.execute(query);

    // select * from sys.databases
    let row = undefined;
    let row_count = 0;
    while (true) {
      row = await connection.nextRow();
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
  const context = {
    serverName: process.env.DB_HOST || 'localhost',
    port: 1433,
    userName: process.env.DB_USER || 'sa',
    password: process.env.SQL_PASSWORD,
    database: 'master',
    trustServerCertificate: true,
  };
  try {
    const connection = await create_connection(context);

    const request = new Request(connection);

    let result = await request.query('select 1; select 10');

    //t.log('Received: ', result.IRecordSet);

    t.assert(result.row_count > 1, 'Expected to fetch 2 rows');
    await connection.close();
    t.pass('Successfully queries using new Request class');
  } catch (err) {
    t.log(err);
    t.fail('Error querying');
  }
});

test('connect to sqlserver and execute parameterized query.', async (t) => {
  // Example TypeScript test with proper typing
  const context = {
    serverName: process.env.DB_HOST || 'localhost',
    port: 1433,
    userName: process.env.DB_USER || 'sa',
    password: process.env.SQL_PASSWORD,
    database: 'master',
    trustServerCertificate: true,
  };

  try {
    const connection = await create_connection(context);
    t.pass('Connection successful');
    let query = 'select * from sys.columns where object_id > @input_parameter;';

    let params = [
      {
        name: '@input_parameter',
        dataType: JsSqlDataTypes.Int,
        value: 3,
      },
    ];

    await connection.execute(query, params);

    let row = undefined;
    let row_count = 0;
    while (true) {
      row = await connection.nextRow();
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

test('decimal and numeric conversion', async (t) => {
  const context = {
    serverName: process.env.DB_HOST || 'localhost',
    port: 1433,
    userName: process.env.DB_USER || 'sa',
    password: process.env.SQL_PASSWORD,
    database: 'master',
    trustServerCertificate: true,
  };
  const connection = await create_connection(context);
  // Test various decimal/numeric values, including large values that require multiple parts
  const testCases = [
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
    const rows = await connection.nextRow();
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
