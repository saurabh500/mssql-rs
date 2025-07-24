// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';

import { create_connection, JsSqlDataTypes, Request } from '../js/index.js';
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

test('execute parameterized query with request class.', async (t) => {
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

    let request = new Request(connection);

    request.input('@input_parameter', JsSqlDataTypes.Int, 3);

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
    let query = 'select @str as string;';

    let request = new Request(connection);

    request.input('@str', JsSqlDataTypes.VarChar, 'test');

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

    let request = new Request(connection);

    request.input('input_parameter', JsSqlDataTypes.Int, 3);

    let result = await request.query(query);

    t.assert(result.rowCount > 1000, 'Expected to fetch more than 1000 rows');
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Connection should succeed');
  }
});

async function openConnection(context) {
  const connection = await create_connection(context);
  if (!connection) {
    throw new Error('Failed to create connection');
  }
  return connection;
}

function createContext() {
  const context = {
    serverName: process.env.DB_HOST || 'localhost',
    port: 1433,
    userName: process.env.DB_USER || 'sa',
    password: process.env.SQL_PASSWORD,
    database: 'master',
    trustServerCertificate: true,
  };
  return context;
}

test('test parameter bool.', async (t) => {
  // Example TypeScript test with proper typing
  const context = createContext();

  try {
    const connection = await openConnection(context);
    t.pass('Connection successful');
    let query = 'SELECT @bit AS bit;';
    let params = [
      {
        name: '@bit',
        dataType: JsSqlDataTypes.Bit,
        value: false,
      },
    ];

    await connection.execute(query, params);

    let row = undefined;
    let row_count = 0;
    let val = undefined;
    while (true) {
      row = await connection.nextRow();
      if (row && row.length > 0) {
        val = row[0].rowVal;
        row_count++;
      } else {
        break;
      }
    }
    t.is(row_count, 1, 'Expected to fetch exactly 1 row');
    t.is(val, false, 'Expected bit value to be false');
    await connection.closeQuery();
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Test Should succeed. Check logged exception');
  }
});

test('test parameter null bool.', async (t) => {
  // Example TypeScript test with proper typing
  const context = createContext();

  try {
    const connection = await openConnection(context);
    t.pass('Connection successful');
    let query = 'SELECT @bit AS bit;';
    let params = [
      {
        name: '@bit',
        dataType: JsSqlDataTypes.Bit,
        value: null,
      },
    ];

    await connection.execute(query, params);

    let row = undefined;
    let row_count = 0;
    let val = undefined;
    while (true) {
      row = await connection.nextRow();
      if (row && row.length > 0) {
        val = row[0].rowVal;
        row_count++;
      } else {
        break;
      }
    }
    t.is(row_count, 1, 'Expected to fetch exactly 1 row');
    t.is(val, null, 'Expected bit value to be null');
    await connection.closeQuery();
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Test Should succeed. Check logged exception');
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
