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
  JsSqlDataTypes.Time,
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
  JsSqlDataTypes.Time,
);

test(
  'test date somevalue in UTC',
  genericMacro,
  new Date(Date.UTC(2023, 0, 1, 12, 34, 56)),
  new Date(Date.UTC(2023, 0, 1, 0, 0, 0)),
  JsSqlDataTypes.Date,
);

test('test date null in UTC', genericMacro, null, null, JsSqlDataTypes.Date);

test(
  'test datetime2 somevalue in UTC',
  genericMacro,
  new Date(Date.UTC(2023, 0, 1, 12, 34, 56)),
  () => {
    let expected = new Date(Date.UTC(2023, 0, 1, 12, 34, 56));
    expected.nanosecondsDelta = 0;
    return expected;
  },
  JsSqlDataTypes.DateTime2,
);

test(
  'test datetime2 null in UTC',
  genericMacro,
  null,
  null,
  JsSqlDataTypes.DateTime2,
);

test(
  'test DateTimeOffset somevalue in UTC',
  genericMacro,
  new Date(Date.UTC(2023, 0, 1, 12, 34, 56)),
  () => {
    let expected = new Date(Date.UTC(2023, 0, 1, 12, 34, 56));
    expected.nanosecondsDelta = 0;
    return expected;
  },
  JsSqlDataTypes.DateTimeOffset,
);

test(
  'test DateTimeOffset null in UTC',
  genericMacro,
  null,
  null,
  JsSqlDataTypes.DateTimeOffset,
);

test(
  'test DateTime somevalue in UTC',
  genericMacro,
  new Date(Date.UTC(2023, 0, 1, 12, 34, 56)),
  new Date(Date.UTC(2023, 0, 1, 12, 34, 56)),
  JsSqlDataTypes.DateTime,
);

test(
  'test DateTime before 1900 in UTC',
  genericMacro,
  new Date(Date.UTC(1800, 0, 1, 12, 34, 56)),
  new Date(Date.UTC(1800, 0, 1, 12, 34, 56)),
  JsSqlDataTypes.DateTime,
);

test(
  'test DateTime null in UTC',
  genericMacro,
  null,
  null,
  JsSqlDataTypes.DateTime,
);

test(
  'test SmallDateTime somevalue in UTC',
  genericMacro,
  new Date(Date.UTC(2023, 0, 1, 12, 34, 56)),
  new Date(Date.UTC(2023, 0, 1, 12, 34)),
  JsSqlDataTypes.SmallDateTime,
);

test(
  'test SmallDateTime null in UTC',
  genericMacro,
  null,
  null,
  JsSqlDataTypes.SmallDateTime,
);
