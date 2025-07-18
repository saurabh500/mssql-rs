// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';
import { create_connection } from '../js/index.js';

test('datetime conversion', async (t) => {
  const context = {
    serverName: process.env.DB_HOST || 'localhost',
    port: 1433,
    userName: process.env.DB_USER || 'sa',
    password: process.env.SQL_PASSWORD,
    database: 'master',
    trustServerCertificate: true,
  };
  const connection = await create_connection(context);
  let expected1 = new Date('1970-01-01T12:34:56.123Z');
  expected1.nanosecondsDelta = 0.0004567; // Example nanoseconds delta
  const testCases = [
    // datetime
    {
      sql: "SELECT CAST('2023-01-01T12:34:56.123' AS DATETIME) AS val",
      expected: new Date('2023-01-01T12:34:56.123Z'),
    },
    {
      sql: "SELECT CAST('1900-01-01T00:00:00.000' AS DATETIME) AS val",
      expected: new Date('1900-01-01T00:00:00.000Z'),
    },
    // smalldatetime
    {
      sql: "SELECT CAST('2023-01-01T12:34:00' AS SMALLDATETIME) AS val",
      expected: new Date('2023-01-01T12:34:00.000Z'),
    },
    {
      sql: "SELECT CAST('2079-06-06T23:59:00' AS SMALLDATETIME) AS val",
      expected: new Date('2079-06-06T23:59:00.000Z'),
    },
    // date
    {
      sql: "SELECT CAST('2022-12-31' AS DATE) AS val",
      expected: new Date('2022-12-31T00:00:00.000Z'),
    },
    {
      sql: "SELECT CAST('0001-01-01' AS DATE) AS val",
      expected: new Date('0001-01-01T00:00:00.000Z'),
    },
    // time
    {
      sql: "SELECT CAST('12:34:56.1234567' AS TIME(7)) AS val",
      expected: () => {
        const date = new Date('1970-01-01T12:34:56.123Z');
        date.nanosecondsDelta = 0.0004567; // Example nanoseconds delta
        return date;
      },
    },
    {
      sql: "SELECT CAST('00:00:00.0000000' AS TIME(7)) AS val",
      expected: () => {
        const date = new Date('1970-01-01T00:00:00.000Z');
        date.nanosecondsDelta = 0;
        return date;
      },
    },
    // datetime2
    {
      sql: "SELECT CAST('2023-01-01T12:34:56.1234567' AS DATETIME2(7)) AS val",
      expected: () => {
        const date = new Date('2023-01-01T12:34:56.123Z');
        date.nanosecondsDelta = 0.0004567;
        return date;
      },
    },
    {
      sql: 'SELECT CAST(NULL AS DATETIME2(7)) AS val',
      expected: null,
    },
    // datetimeoffset
    {
      sql: "SELECT CAST('2023-01-01T12:34:56.1234567+05:30' AS DATETIMEOFFSET(7)) AS val",
      expected: () => {
        const date = new Date('2023-01-01T07:04:56.123Z');
        date.nanosecondsDelta = 0.0004567;
        return date;
      },
    },
    {
      sql: 'SELECT CAST(NULL AS DATETIMEOFFSET(7)) AS val',
      expected: null,
    },
  ];
  for (const { sql, expected } of testCases) {
    await connection.execute(sql);
    const rows = await connection.nextRow();
    t.truthy(rows && rows.length > 0, `Should return a row for: ${sql}`);
    const val = rows[0].rowVal;
    if (expected == null) {
      t.is(val, null, `Expected null value for: ${sql}`);
    } else if (expected instanceof Date) {
      t.deepEqual(val, expected, `Expected datetime value for: ${sql}`);
    } else if (typeof expected === 'function') {
      const expectedDate = expected();
      t.deepEqual(val, expectedDate, `Expected function result for: ${sql}`);
    } else if (
      typeof expected === 'object' &&
      expected.nanosecondsDelta !== undefined
    ) {
      t.deepEqual(
        val,
        expected,
        `Expected datetime2 value with nanoseconds delta for: ${sql}`,
      );
    } else if (typeof expected === 'string') {
      t.is(val, expected, `Expected string value for: ${sql}`);
    } else {
      t.is(val, expected, `Expected datetime value for: ${sql}`);
    }
    await connection.closeQuery();
  }
  await connection.close();
});
