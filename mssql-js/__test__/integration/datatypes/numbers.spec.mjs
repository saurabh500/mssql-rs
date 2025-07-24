// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';
import { createContext, openConnection } from '../../db.mjs';
import { JsSqlDataTypes, Request } from '../../../js/index.js';

async function executeScalar(request, query) {
  const result = await request.query(query);
  let firstRowDictionary = Object.values(result.IRecordSet)[0];
  return Object.values(firstRowDictionary)[0];
}

async function runTinyIntTest(t, inputValue, expectedValue, sqlType) {
  const connection = await openConnection(createContext());
  try {
    let query = 'select @param;';
    let request = new Request(connection);
    request.input('@param', sqlType, inputValue);
    let val = await executeScalar(request, query);
    t.is(val, expectedValue, `Expected value to be ${expectedValue}`);
    t.pass('Query executed successfully');
  } finally {
    await connection.close();
  }
}

const genericMacro = async (t, inputValue, expectedValue, sqlType) => {
  await runTinyIntTest(t, inputValue, expectedValue, sqlType);
};

test('test tinyint somevalue', genericMacro, 123, 123, JsSqlDataTypes.TinyInt);
test('test tinyint null', genericMacro, null, null, JsSqlDataTypes.TinyInt);

test('test tinyint negative', async (t) => {
  const connection = await openConnection(createContext());
  try {
    let query = 'select @param;';
    let request = new Request(connection);
    request.input('@param', JsSqlDataTypes.TinyInt, -123);
    await t.throwsAsync(
      () => executeScalar(request, query),
      undefined,
      'Expected error for negative tinyint value',
    );
  } finally {
    await connection.close();
  }
});

test('test int somevalue', genericMacro, 123, 123, JsSqlDataTypes.Int);
test('test int null', genericMacro, null, null, JsSqlDataTypes.Int);
test('test int negative', genericMacro, -123, -123, JsSqlDataTypes.Int);

test('test bigint somevalue', genericMacro, 123, 123n, JsSqlDataTypes.BigInt);
test('test bigint null', genericMacro, null, null, JsSqlDataTypes.BigInt);
test('test bigint negative', genericMacro, -123, -123n, JsSqlDataTypes.BigInt);
