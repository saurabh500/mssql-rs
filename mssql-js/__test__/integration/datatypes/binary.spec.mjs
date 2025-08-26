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

test('test varbinary size with request class.', async (t) => {
  try {
    const connection = await openConnection(await createContext());
    t.pass('Connection successful');
    let query = 'select @bin as binary;';

    let request = new Request(connection);

    const buffer = Buffer.from("48656C6C6F", "hex");

    request.input('@bin', TYPES.VarBinary(5), buffer);

    let result = await request.query(query);

    t.assert(result.rowCount === 1, 'Expected to fetch exactly 1 row');
    let firstRowDictionary = Object.values(result.IRecordSet)[0];
    t.deepEqual(firstRowDictionary.binary, Buffer.from("48656C6C6F", "hex"), `Expected the same buffer`);
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Connection should succeed');
  }
});

test('test varbinary size and smaller data with request class.', async (t) => {
  try {
    const connection = await openConnection(await createContext());
    t.pass('Connection successful');
    let query = 'select @bin as binary;';

    let request = new Request(connection);

    const buffer = Buffer.from("4865", "hex");

    request.input('@bin', TYPES.VarBinary(5), buffer);

    let result = await request.query(query);

    t.assert(result.rowCount === 1, 'Expected to fetch exactly 1 row');
    let firstRowDictionary = Object.values(result.IRecordSet)[0];
    t.deepEqual(firstRowDictionary.binary, Buffer.from("4865", "hex"), `Expected the same buffer`);
    await connection.close();
    t.pass('Query executed successfully');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Connection should succeed');
  }
});

test('varbinary with various sizes.', async (t) => {
  // Test with different sizes
  const sizes = [1, 10, 100, 1000, 4000];
  for (const size of sizes) {
    try {
      let connection = await openConnection(await createContext());
      let query = 'select @bin as binary;';
      let request = new Request(connection);
      const binaryString = 'a'.repeat(size);
      const inputBuffer = Buffer.from(binaryString);
      request.input('@bin', TYPES.VarBinary(size), inputBuffer);
      let result = await request.query(query);
      t.assert(
        result.rowCount === 1,
        `Expected to fetch exactly 1 row for size ${size}`,
      );
      let firstRowDictionary = Object.values(result.IRecordSet)[0];
      t.deepEqual(
        firstRowDictionary.binary,
        inputBuffer,
        `Expected buffer to match for size ${size}`,
      );
      await connection.close();
    } catch (error) {
      t.log(`Connection failed for size ${size}:`, error);
      t.fail(`Connection should succeed for size ${size}`);
    }
    t.pass('All NVarChar size tests passed successfully');
  }
});

test('test failing varbinary size with request class.', async (t) => {
  try {
    const connection = await openConnection(await createContext());
    t.pass('Connection successful');
    let query = 'select @bin as binary;';

    let request = new Request(connection);

    const buffer = Buffer.from("48656C6C6F20", "hex");

    request.input('@bin', TYPES.VarBinary(5), buffer);

    //expect the query to throw an error if the data is larger than the length
    await t.throwsAsync(request.query(query)); 
    await connection.close();
    t.pass('Query correctly fails');
  } catch (error) {
    t.log('Connection failed:', error);
    t.fail('Connection should succeed');
  }
});
