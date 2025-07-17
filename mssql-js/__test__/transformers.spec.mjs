// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';
import { JsSqlDataTypes, tdsToJsTransformers } from '../js/index.js';

// Mocks for Metadata and Buffer
const mockMetadata = (dataType, encoding = undefined) => ({
  name: 'col',
  dataType,
  encoding,
});
const mockBuffer = (arr) => Buffer.from(arr);

// --- Date/Time tests ---
test('smallDateTimeTransformer', (t) => {
  const transformer = tdsToJsTransformers[JsSqlDataTypes.SmallDateTime];
  const row = { days: 10, time: 30 };
  const result = transformer(mockMetadata(JsSqlDataTypes.SmallDateTime), row);
  t.true(result instanceof Date);
});

test('dateTimeTransformer', (t) => {
  const transformer = tdsToJsTransformers[JsSqlDataTypes.DateTime];
  const row = { days: 10, time: 30 };
  const result = transformer(mockMetadata(JsSqlDataTypes.DateTime), row);
  t.true(result instanceof Date);
});

test('dateTransformer', (t) => {
  const transformer = tdsToJsTransformers[JsSqlDataTypes.Date];
  const row = 730128; // 2000-01-11
  const result = transformer(mockMetadata(JsSqlDataTypes.Date), row);
  t.true(result instanceof Date);
});

// --- String tests ---
test('nCharNVarCharTransformer', (t) => {
  const transformer = tdsToJsTransformers[JsSqlDataTypes.NVarChar];
  const str = 'hello';
  const buf = Buffer.from(str, 'ucs2');
  const result = transformer(mockMetadata(JsSqlDataTypes.NVarChar), buf);
  t.is(result, str);
});

test('varCharTransformer', (t) => {
  const transformer = tdsToJsTransformers[JsSqlDataTypes.VarChar];
  const str = 'world';
  const buf = Buffer.from(str, 'utf8');
  const result = transformer(
    mockMetadata(JsSqlDataTypes.VarChar, {
      isUtf8: true,
      sortId: 0,
      lcidLanguageId: 0,
    }),
    buf,
  );
  t.is(result, str);
});

// --- Binary test ---
test('binaryTransformer', (t) => {
  const transformer = tdsToJsTransformers[JsSqlDataTypes.VarBinary];
  const buf = mockBuffer([1, 2, 3]);
  const result = transformer(mockMetadata(JsSqlDataTypes.VarBinary), buf);
  t.deepEqual(result, buf);
});

// --- Boolean test ---
test('bitTransformer', (t) => {
  const transformer = tdsToJsTransformers[JsSqlDataTypes.Bit];
  t.is(transformer(mockMetadata(JsSqlDataTypes.Bit), true), true);
  t.is(transformer(mockMetadata(JsSqlDataTypes.Bit), false), false);
});

// --- Integer test ---
test('intTransformer', (t) => {
  const transformer = tdsToJsTransformers[JsSqlDataTypes.Int4];
  t.is(transformer(mockMetadata(JsSqlDataTypes.Int4), 42), 42);
});

test('bigintTransformer', (t) => {
  const transformer = tdsToJsTransformers[JsSqlDataTypes.Int8];
  t.is(
    transformer(mockMetadata(JsSqlDataTypes.Int8), 1234567890123456789n),
    1234567890123456789n,
  );
});

// --- Money test ---
test('moneyTransformer', (t) => {
  const transformer = tdsToJsTransformers[JsSqlDataTypes.Money];
  const row = { lsbPart: 10000, msbPart: 0 };
  t.is(transformer(mockMetadata(JsSqlDataTypes.Money), row), 1);
});

test('smallMoneyTransformer', (t) => {
  const transformer = tdsToJsTransformers[JsSqlDataTypes.SmallMoney];
  t.is(transformer(mockMetadata(JsSqlDataTypes.SmallMoney), 20000), 2);
});

// --- Guid test ---
test('guidTransformer', (t) => {
  const transformer = tdsToJsTransformers[JsSqlDataTypes.Guid];
  const str = 'guid-test';
  const buf = Buffer.from(str, 'utf8');
  t.is(transformer(mockMetadata(JsSqlDataTypes.Guid), buf), str);
});

// --- Float test ---
test('floatTransformer', (t) => {
  const transformer = tdsToJsTransformers[JsSqlDataTypes.Flt8];
  t.is(transformer(mockMetadata(JsSqlDataTypes.Flt8), 3.14), 3.14);
});
