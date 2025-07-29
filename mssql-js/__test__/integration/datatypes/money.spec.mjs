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

test(
  'test money negative',
  genericMacro,
  -456789012345.6789,
  -456789012345.6789,
  JsSqlDataTypes.Money,
);

test(
  'test smallmoney somevalue',
  genericMacro,
  123,
  123,
  JsSqlDataTypes.SmallMoney,
);
test(
  'test smallmoney null',
  genericMacro,
  null,
  null,
  JsSqlDataTypes.SmallMoney,
);
test(
  'test smallmoney negative',
  genericMacro,
  -123,
  -123,
  JsSqlDataTypes.SmallMoney,
);

test(
  'test money somevalue',
  genericMacro,
  456789012345.6789,
  456789012345.6789,
  JsSqlDataTypes.Money,
);
test('test money null', genericMacro, null, null, JsSqlDataTypes.Money);
