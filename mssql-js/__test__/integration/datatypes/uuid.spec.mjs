// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';
import { createContext, openConnection } from '../../db.mjs';
import { JsSqlDataTypes, Request } from '../../../js/index.js';
import { TYPES } from '../../../js/datatypes/types.js';

async function executeScalar(request, query) {
  const result = await request.query(query);
  let firstRowDictionary = Object.values(result.IRecordSet)[0];
  return Object.values(firstRowDictionary)[0];
}

async function runTest(t, inputValue, expectedValue, sqlType) {
  const connection = await openConnection(createContext());
  try {
    let query = 'select @param;';
    let request = new Request(connection);
    request.input('@param', sqlType, inputValue);
    let val = await executeScalar(request, query);
    if (expectedValue instanceof Function) {
      expectedValue = expectedValue();
    }
    t.deepEqual(val, expectedValue, `Expected value to be ${expectedValue}`);
    t.pass('Query executed successfully');
  } finally {
    await connection.close();
  }
}

const genericMacro = async (t, inputValue, expectedValue, sqlType) => {
  await runTest(t, inputValue, expectedValue, sqlType);
};

test(
  'test guid somevalue',
  genericMacro,
  '550e8400-e29b-41d4-a716-446655440000',
  '550e8400-e29b-41d4-a716-446655440000',
  TYPES.UniqueIdentifier,
);

test('test GUID null ', genericMacro, null, null, TYPES.UniqueIdentifier);
