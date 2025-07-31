// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';

import { createContext, openConnection } from '../../db.mjs';
import { JsSqlDataTypes, Request } from '../../../js/index.js';
import { TYPES } from '../../../js/datatypes/types.js';

test('test varchar with request class.', async (t) => {
  try {
    const connection = await openConnection(createContext());
    t.pass('Connection successful');
    let query = 'select @str as string;';

    let request = new Request(connection);

    request.input('@str', TYPES.VarChar, 'test');

    let result = await request.query(query);

    t.assert(result.rowCount === 1, 'Expected to fetch exactly 1 row');
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Connection should succeed');
  }
});

test('test nvarchar with request class.', async (t) => {
  try {
    const connection = await openConnection(createContext());
    t.pass('Connection successful');
    let query = 'select @str as string;';

    let request = new Request(connection);

    const emojiString = 'Hello 👋 World 🌍';
    request.input('@str', TYPES.NVarChar, emojiString);

    let result = await request.query(query);

    t.assert(result.rowCount === 1, 'Expected to fetch exactly 1 row');
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Connection should succeed');
  }
});
