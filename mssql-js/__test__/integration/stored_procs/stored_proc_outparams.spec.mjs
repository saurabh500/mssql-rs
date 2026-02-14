// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';

import { createContext } from '../../db.mjs';

import { create_connection, Request } from '../../../dist/index.js';
import { TYPES } from '../../../dist/datatypes/types.js';

test('Get output parameters int from stored procedure', async (t) => {
  try {
    const connection = await create_connection(await createContext());
    t.pass('Connection successful');

    let query =
      'CREATE PROCEDURE #test_proc \
        @paramIn int, \
        @paramOut int output \
        AS \
        BEGIN \
          select 1 \
          set @paramOut = @paramIn \
        END';
    t.log('Creating stored procedure:', query);
    let request = new Request(connection);

    // Create the stored procedure
    await request.query(query);

    request = new Request(connection);

    let inputoutputvalue = Math.floor(Math.random() * 100);
    request.input('@paramIn', TYPES.Int, inputoutputvalue);
    request.output('paramOut', TYPES.Int);

    let result = await request.execute('#test_proc');

    t.assert(result.rowCount > 0, 'Expected to fetch at least 1 row');
    t.assert(result.output != null, 'Expected output to be not null');
    t.assert(
      Object.keys(result.output).length > 0,
      'Expected output dictionary to have at least one key',
    );
    t.assert(
      result.output.hasOwnProperty('paramOut'),
      'Expected output to have key paramOut',
    );
    t.assert(
      result.output.paramOut === inputoutputvalue,
      `Expected output parameter to match input value, got ${result.output.paramOut}`,
    );
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Connection should succeed');
  }
});

test('Get output parameters varchar from stored procedure', async (t) => {
  try {
    const connection = await create_connection(await createContext());
    t.pass('Connection successful');

    let query =
      'CREATE PROCEDURE #test_proc \
        @paramIn varchar(255), \
        @paramOut varchar(255) output \
        AS \
        BEGIN \
          select 1 \
          set @paramOut = @paramIn \
        END';
    t.log('Creating stored procedure:', query);
    let request = new Request(connection);

    // Create the stored procedure
    await request.query(query);

    request = new Request(connection);

    let inputoutputvalue = 'hello there this is a test string';
    request.input('@paramIn', TYPES.VarChar, inputoutputvalue);
    request.output('paramOut', TYPES.VarChar);

    let result = await request.execute('#test_proc');

    t.assert(result.rowCount > 0, 'Expected to fetch at least 1 row');
    t.assert(result.output != null, 'Expected output to be not null');
    t.assert(
      Object.keys(result.output).length > 0,
      'Expected output dictionary to have at least one key',
    );
    t.assert(
      result.output.hasOwnProperty('paramOut'),
      'Expected output to have key paramOut',
    );
    t.assert(
      result.output.paramOut === inputoutputvalue,
      `Expected output parameter to match input value, got ${result.output.paramOut}`,
    );
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Connection should succeed');
  }
});
