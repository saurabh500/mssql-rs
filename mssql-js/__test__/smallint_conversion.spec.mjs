// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';
import { createContext, openConnection } from './db.mjs';

test('smallint conversion', async (t) => {
  const connection = await openConnection(await createContext());
  const testCases = [
    { sql: 'SELECT CAST(-32768 AS SMALLINT) AS val', expected: -32768 },
    { sql: 'SELECT CAST(32767 AS SMALLINT) AS val', expected: 32767 },
    { sql: 'SELECT CAST(0 AS SMALLINT) AS val', expected: 0 },
  ];
  for (const { sql, expected } of testCases) {
    await connection.execute(sql);
    const rows = await connection.nextRow();
    t.truthy(rows && rows.length > 0, `Should return a row for: ${sql}`);
    const val = rows[0].rowVal;
    t.is(Number(val), expected, `Expected smallint value for: ${sql}`);
    await connection.closeQuery();
  }
  await connection.close();
});
