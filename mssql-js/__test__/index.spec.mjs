// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';

import { create_connection } from '../js/index.js';

test('connect to sqlserver', async (t) => {
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
      "SELECT CAST('SOMETHING SOMETHING SOMETHING SOMETHING' AS VARCHAR(MAX)) COLLATE Latin1_General_100_CI_AS_SC_UTF8;";
    await connection.execute(query);

    // select * from sys.databases
    let row = undefined;
    let row_count = 0;
    while (true) {
      row = await connection.nextRow();
      if (row && row.length > 0) {
        // t.log('Row fetched:', row);
        row_count++;
        console.log(row);
      } else {
        break;
      }
    }
    t.assert(row_count > 0, 'Expected to fetch some rows');
    t.log(`Total rows fetched: ${row_count}`);
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
