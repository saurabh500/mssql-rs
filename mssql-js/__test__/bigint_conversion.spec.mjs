// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';
import { createContext, nextRow, openConnection } from './db.mjs';

test('bigint conversion', async (t) => {
  const connection = await openConnection(await createContext());
  const testCases = [
    {
      sql: 'SELECT CAST(-9223372036854775808 AS BIGINT) AS val',
      expected: -9223372036854775808n,
    },
    {
      sql: 'SELECT CAST(9223372036854775807 AS BIGINT) AS val',
      expected: 9223372036854775807n,
    },
    { sql: 'SELECT CAST(0 AS BIGINT) AS val', expected: 0n },
  ];
  for (const { sql, expected } of testCases) {
    await connection.execute(sql);
    const rows = await nextRow(connection);
    t.truthy(rows && rows.length > 0, `Should return a row for: ${sql}`);
    const val = rows[0].rowVal;
    t.is(BigInt(val), expected, `Expected bigint value for: ${sql}`);
    await connection.closeQuery();
  }
  await connection.close();
});
