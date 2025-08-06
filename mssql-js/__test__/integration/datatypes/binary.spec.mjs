// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';

import { createContext, openConnection } from '../../db.mjs';
import { Request } from '../../../dist/index.js';
import { TYPES } from '../../../dist/datatypes/types.js';

test('test varbinary some value with request class.', async (t) => {
  try {
    const connection = await openConnection(await createContext());
    t.pass('Connection successful');
    let query = 'select @bin as binary;';

    let request = new Request(connection);

    // Buffer with 30 elements.
    const buffer = Buffer.from(Array.from({ length: 30 }, (_, i) => i + 1));
    request.input('@bin', TYPES.VarBinary, buffer);

    let result = await request.query(query);

    t.assert(result.rowCount === 1, 'Expected to fetch exactly 1 row');
    let firstRowDictionary = Object.values(result.IRecordSet)[0];
    t.deepEqual(
      firstRowDictionary.binary,
      buffer,
      `Expected Buffer to match for size ${buffer.length}`,
    );
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Connection should succeed');
  }
});

test('test varbinary null value with request class.', async (t) => {
  try {
    const connection = await openConnection(await createContext());
    t.pass('Connection successful');
    let query = 'select @bin as binary;';

    let request = new Request(connection);

    request.input('@bin', TYPES.VarBinary(-1), null);

    let result = await request.query(query);

    t.assert(result.rowCount === 1, 'Expected to fetch exactly 1 row');
    let firstRowDictionary = Object.values(result.IRecordSet)[0];
    t.deepEqual(firstRowDictionary.binary, null, `Expected null buffer`);
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Connection should succeed');
  }
});
