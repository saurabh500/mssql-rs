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
  'test time somevalue in UTC',
  genericMacro,
  new Date(Date.UTC(2023, 0, 1, 12, 34, 56)),
  () => {
    let expected = new Date(Date.UTC(1970, 0, 1, 12, 34, 56));
    expected.nanosecondsDelta = 0;
    return expected;
  },
  TYPES.Time,
);

test(
  'test time somevalue ',
  genericMacro,
  new Date(2023, 0, 1, 12, 34, 56),
  () => {
    let expected = new Date(1970, 0, 1, 12, 34, 56);
    expected.nanosecondsDelta = 0;
    return expected;
  },
  TYPES.Time,
);

test('test time null ', genericMacro, null, null, TYPES.Time);

test(
  'test date somevalue in UTC',
  genericMacro,
  new Date(Date.UTC(2023, 0, 1, 12, 34, 56)),
  new Date(Date.UTC(2023, 0, 1, 0, 0, 0)),
  TYPES.Date,
);

test('test date null in UTC', genericMacro, null, null, TYPES.Date);

test(
  'test datetime2 somevalue in UTC',
  genericMacro,
  new Date(Date.UTC(2023, 0, 1, 12, 34, 56)),
  () => {
    let expected = new Date(Date.UTC(2023, 0, 1, 12, 34, 56));
    expected.nanosecondsDelta = 0;
    return expected;
  },
  TYPES.DateTime2,
);

test('test datetime2 null in UTC', genericMacro, null, null, TYPES.DateTime2);

test(
  'test DateTimeOffset somevalue in UTC',
  genericMacro,
  new Date(Date.UTC(2023, 0, 1, 12, 34, 56)),
  () => {
    let expected = new Date(Date.UTC(2023, 0, 1, 12, 34, 56));
    expected.nanosecondsDelta = 0;
    return expected;
  },
  TYPES.DateTimeOffset,
);

test(
  'test DateTimeOffset null in UTC',
  genericMacro,
  null,
  null,
  TYPES.DateTimeOffset,
);

test(
  'test DateTime somevalue in UTC',
  genericMacro,
  new Date(Date.UTC(2023, 0, 1, 12, 34, 56)),
  new Date(Date.UTC(2023, 0, 1, 12, 34, 56)),
  TYPES.DateTime,
);

test(
  'test DateTime before 1900 in UTC',
  genericMacro,
  new Date(Date.UTC(1800, 0, 1, 12, 34, 56)),
  new Date(Date.UTC(1800, 0, 1, 12, 34, 56)),
  TYPES.DateTime,
);

test('test DateTime null in UTC', genericMacro, null, null, TYPES.DateTime);

test(
  'test SmallDateTime somevalue in UTC',
  genericMacro,
  new Date(Date.UTC(2023, 0, 1, 12, 34, 56)),
  new Date(Date.UTC(2023, 0, 1, 12, 34)),
  TYPES.SmallDateTime,
);

test(
  'test SmallDateTime null in UTC',
  genericMacro,
  null,
  null,
  TYPES.SmallDateTime,
);
