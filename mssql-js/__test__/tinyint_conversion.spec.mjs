// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';
import { create_connection } from '../js/index.js';

test('tinyint conversion', async (t) => {
  const context = {
    serverName: process.env.DB_HOST || 'localhost',
    port: 1433,
    userName: process.env.DB_USER || 'sa',
    password: process.env.SQL_PASSWORD,
    database: 'master',
    trustServerCertificate: true,
  };
  const connection = await create_connection(context);
  const testCases = [
    { sql: 'SELECT CAST(0 AS TINYINT) AS val', expected: 0 },
    { sql: 'SELECT CAST(255 AS TINYINT) AS val', expected: 255 },
    { sql: 'SELECT CAST(128 AS TINYINT) AS val', expected: 128 },
  ];
  for (const { sql, expected } of testCases) {
    await connection.execute(sql);
    const rows = await connection.nextRow();
    t.truthy(rows && rows.length > 0, `Should return a row for: ${sql}`);
    const val = rows[0].rowVal;
    t.is(Number(val), expected, `Expected tinyint value for: ${sql}`);
    await connection.closeQuery();
  }
  await connection.close();
});
