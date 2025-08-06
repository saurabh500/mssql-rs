// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';

import { createContext, openConnection } from '../../db.mjs';
import { Request } from '../../../dist/index.js';
import { TYPES } from '../../../dist/datatypes/types.js';

test('test varchar with request class.', async (t) => {
  try {
    const connection = await openConnection(await createContext());
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
    const connection = await openConnection(await createContext());
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

test('Nvarchar with various sizes.', async (t) => {
  // Test with different sizes
  const sizes = [1, 10, 100, 1000, 4000];
  for (const size of sizes) {
    try {
      let connection = await openConnection(await createContext());
      let query = 'select @str as string;';
      let request = new Request(connection);
      const inputString = 'a'.repeat(size);
      request.input('@str', TYPES.NVarChar(size), inputString);
      let result = await request.query(query);
      t.assert(
        result.rowCount === 1,
        `Expected to fetch exactly 1 row for size ${size}`,
      );
      let firstRowDictionary = Object.values(result.IRecordSet)[0];
      t.is(
        firstRowDictionary.string,
        inputString,
        `Expected string to match for size ${size}`,
      );
      await connection.close();
    } catch (error) {
      t.log(`Connection failed for size ${size}:`, error);
      t.fail(`Connection should succeed for size ${size}`);
    }
    t.pass('All NVarChar size tests passed successfully');
  }
});

test('test nvarchar(-1) with request class.', async (t) => {
  try {
    let connection = await openConnection(await createContext());
    let query = 'select @str as string;';
    let request = new Request(connection);
    const inputString = 'a'.repeat(30000);
    request.input('@str', TYPES.NVarChar(-1), inputString);
    let result = await request.query(query);
    t.assert(result.rowCount === 1, `Expected to fetch exactly 1 row for MAX`);
    let firstRowDictionary = Object.values(result.IRecordSet)[0];
    t.is(
      firstRowDictionary.string,
      inputString,
      `Expected string to match for size MAX`,
    );
    await connection.close();
  } catch (error) {
    t.log(`Test failed for MAX:`, error);
    t.fail(`Test should succeed for size -1`);
  }
  t.pass('All NVarChar size tests passed successfully');
});

test('test nvarchar NULL with request class.', async (t) => {
  try {
    let connection = await openConnection(await createContext());
    let query = 'select @str as string;';
    let request = new Request(connection);
    request.input('@str', TYPES.NVarChar(-1), null);
    let result = await request.query(query);
    t.assert(result.rowCount === 1, `Expected to fetch exactly 1 row for MAX`);
    let firstRowDictionary = Object.values(result.IRecordSet)[0];
    t.is(firstRowDictionary.string, null, `Expected null for MAX`);
    await connection.close();
  } catch (error) {
    t.log(`Test failed for MAX:`, error);
    t.fail(`Test should succeed for null`);
  }
  t.pass('All NVarChar size tests passed successfully');
});
