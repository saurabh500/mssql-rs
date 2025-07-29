// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';
import { createContext, openConnection } from '../../db.mjs';
import { JsSqlDataTypes, Request } from '../../../js/index.js';
import { match } from 'assert';

async function executeScalar(request, query) {
  const result = await request.query(query);
  let firstRowDictionary = Object.values(result.IRecordSet)[0];
  return Object.values(firstRowDictionary)[0];
}

async function runNumberQueryTest(
  t,
  inputValue,
  expectedValue,
  sqlType,
  precision,
  scale,
  transform,
) {
  const connection = await openConnection(createContext());
  try {
    let query = 'select @param;';
    let request = new Request(connection);
    request.input('@param', sqlType, inputValue);
    let val = await executeScalar(request, query);
    if (transform) {
      val = transform(val);
    }
    t.is(val, expectedValue, `Expected value to be ${expectedValue}`);
    t.pass('Query executed successfully');
  } finally {
    await connection.close();
  }
}

const genericMacro = async (
  t,
  inputValue,
  expectedValue,
  sqlType,
  precision = 18,
  scale = 0,
  transformServeValueBeforeCompare = (value) => value,
) => {
  await runNumberQueryTest(
    t,
    inputValue,
    expectedValue,
    sqlType,
    precision,
    scale,
    transformServeValueBeforeCompare,
  );
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

test(
  'test decimal negative',
  genericMacro,
  -123.45,
  // Default scale is 0 and precision is 18
  -123,
  JsSqlDataTypes.Decimal,
);

test(
  'test decimal positive',
  genericMacro,
  12323123123.45,
  // Default scale is 0 and precision is 18
  12323123123,
  JsSqlDataTypes.Decimal,
);

test('test decimal null', genericMacro, null, null, JsSqlDataTypes.Decimal);

test('test numeric null', genericMacro, null, null, JsSqlDataTypes.Numeric);

test(
  'test numeric negative',
  genericMacro,
  -123.45,
  // Default scale is 0 and precision is 18
  -123,
  JsSqlDataTypes.Numeric,
);

test(
  'test numeric positive',
  genericMacro,
  12323123123.45,
  // Default scale is 0 and precision is 18
  12323123123,
  JsSqlDataTypes.Decimal,
);

test(
  'test smallint somevalue',
  genericMacro,
  123,
  123,
  JsSqlDataTypes.SmallInt,
);

test('test smallint null', genericMacro, null, null, JsSqlDataTypes.SmallInt);

test(
  'test smallint negative',
  genericMacro,
  -123,
  -123,
  JsSqlDataTypes.SmallInt,
);

test(
  'test real somevalue',
  genericMacro,
  123.45,
  123.45,
  JsSqlDataTypes.Real,
  undefined,
  undefined,
  (value) => {
    return Math.round(value * 100) / 100;
  },
);
test('test real null', genericMacro, null, null, JsSqlDataTypes.Real);
test(
  'test real negative',
  genericMacro,
  -123.45,
  -123.45,
  JsSqlDataTypes.Real,
  undefined,
  undefined,
  (value) => {
    return Math.round(value * 100) / 100;
  },
);

test(
  'test float somevalue',
  genericMacro,
  123.45,
  123.45,
  JsSqlDataTypes.Float,
);

test('test float null', genericMacro, null, null, JsSqlDataTypes.Float);

test(
  'test float negative',
  genericMacro,
  -123.45,
  -123.45,
  JsSqlDataTypes.Float,
);
