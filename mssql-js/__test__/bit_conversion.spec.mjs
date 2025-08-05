// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';
import { createContext, nextRow, openConnection } from './db.mjs';

test('bit conversion', async (t) => {
  const connection = await openConnection(await createContext());
  const testCases = [
    { sql: 'SELECT CAST(1 AS BIT) AS val', expected: true },
    { sql: 'SELECT CAST(0 AS BIT) AS val', expected: false },
  ];
  for (const { sql, expected } of testCases) {
    await connection.execute(sql);
    const rows = await nextRow(connection);
    t.truthy(rows && rows.length > 0, `Should return a row for: ${sql}`);
    const val = rows[0].rowVal;
    t.is(Boolean(val), expected, `Expected bit value for: ${sql}`);
    await connection.closeQuery();
  }
  await connection.close();
});
