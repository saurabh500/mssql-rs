// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';
import { create_connection } from '../js/index.js';

test('bit conversion', async (t) => {
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
    { sql: 'SELECT CAST(1 AS BIT) AS val', expected: true },
    { sql: 'SELECT CAST(0 AS BIT) AS val', expected: false },
  ];
  for (const { sql, expected } of testCases) {
    await connection.execute(sql);
    const rows = await connection.nextRow();
    t.truthy(rows && rows.length > 0, `Should return a row for: ${sql}`);
    const val = rows[0].rowVal;
    t.is(Boolean(val), expected, `Expected bit value for: ${sql}`);
    await connection.closeQuery();
  }
  await connection.close();
});
