// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';
import { createContext, nextRow, openConnection } from './db.mjs';

test('int conversion', async (t) => {
  const connection = await openConnection(await createContext());
  const testCases = [
    { sql: 'SELECT CAST(-2147483648 AS INT) AS val', expected: -2147483648 },
    { sql: 'SELECT CAST(2147483647 AS INT) AS val', expected: 2147483647 },
    { sql: 'SELECT CAST(0 AS INT) AS val', expected: 0 },
  ];
  for (const { sql, expected } of testCases) {
    await connection.execute(sql);
    const rows = await nextRow(connection);
    t.truthy(rows && rows.length > 0, `Should return a row for: ${sql}`);
    const val = rows[0].rowVal;
    t.is(Number(val), expected, `Expected int value for: ${sql}`);
    await connection.closeQuery();
  }
  await connection.close();
});
