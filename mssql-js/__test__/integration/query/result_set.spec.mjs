// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
import test from 'ava';

import { createContext } from '../../db.mjs';

import { create_connection, Request } from '../../../js/index.js';

test('querying anonymous columns', async (t) => {
  // Example TypeScript test with proper typing
  try {
    const connection = await create_connection(await createContext());
    t.pass('Connection successful');
    let query = 'select 1, 2;';

    let request = new Request(connection);

    let result = await request.query(query);

    t.assert(result.rowCount === 1, 'Expected to fetch 1 row');
    t.assert(
      Array.isArray(result.IRecordSet[0]['']),
      'Expected more than one column to be in an array',
    );
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Connection should succeed');
  }
});

test('querying more than 2 anonymous columns', async (t) => {
  // Example TypeScript test with proper typing

  try {
    const connection = await create_connection(await createContext());
    t.pass('Connection successful');
    let query = 'select 1, 2, 4, 5;';

    let request = new Request(connection);

    let result = await request.query(query);

    t.assert(result.rowCount === 1, 'Expected to fetch 1 row');
    t.assert(
      result.IRecordSet.columns.length === 1,
      'Expected to have only one unnamed column',
    );
    t.assert(
      Array.isArray(result.IRecordSet[0]['']),
      'Expected more than one column to be in an array',
    );
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Connection should succeed');
  }
});

test('querying result set with only one anonymous column', async (t) => {
  // Example TypeScript test with proper typing

  try {
    const connection = await create_connection(await createContext());
    t.pass('Connection successful');
    let query = 'select 1, 2; select 10;';

    let request = new Request(connection);

    let result = await request.query(query);

    t.assert(result.rowCount === 2, 'Expected to fetch 2 rows');
    t.assert(
      !Array.isArray(result.IRecordSets[1][0]['']),
      'Expected one unnamed column to be alone',
    );
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Connection should succeed');
  }
});

test('testing number of columns', async (t) => {
  // Example TypeScript test with proper typing

  try {
    const connection = await create_connection(await createContext());
    t.pass('Connection successful');
    let query =
      'select 1, 2, 3, 4, 5; select 10,  name from sys.columns where object_id = 3;';

    let request = new Request(connection);

    let result = await request.query(query);

    t.assert(result.rowCount === 15, 'Expected to fetch 15 rows');
    t.assert(
      result.IRecordSet.columns.length === 1,
      'Expected to have only one unnamed column',
    );
    t.assert(
      result.IRecordSets[1].columns.length === 2,
      'Expected regular columns to be seperate from unnamed columns',
    );
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Connection should succeed');
  }
});
