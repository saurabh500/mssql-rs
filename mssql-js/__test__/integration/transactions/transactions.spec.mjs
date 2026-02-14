// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import test from 'ava';
import { createContext, openConnection } from '../../db.mjs';
import { Request, Transaction } from '../../../dist/index.js';

test('transaction commit', async (t) => {
  const connection = await openConnection(await createContext());
  try {
    let transaction = new Transaction(connection);
    await transaction.begin();
    const request = new Request(connection);
    await request.query('CREATE TABLE #TestTable (Id INT, Name NVARCHAR(50))');
    await request.query("INSERT INTO #TestTable (Id, Name) VALUES (1, 'Test')");
    await transaction.commit();

    const result = await request.query('SELECT * FROM #TestTable');
    let firstRowDictionary = Object.values(result.IRecordSet)[0];
    let scalarValue = Object.values(firstRowDictionary)[0];
    t.is(scalarValue, 1, 'Expected to insert one row with Id 1');
  } finally {
    await connection.close();
  }
});

test('transaction rollback', async (t) => {
  const connection = await openConnection(await createContext());
  try {
    let transaction = new Transaction(connection);
    const request = new Request(connection);
    await request.query('CREATE TABLE #TestTable (Id INT, Name NVARCHAR(50))');
    await transaction.begin();
    await request.query("INSERT INTO #TestTable (Id, Name) VALUES (1, 'Test')");
    await transaction.rollback();

    const result = await request.query('SELECT * FROM #TestTable');
    let firstRowDictionary = Object.values(result.IRecordSet)[0];

    // Check if firstRowDictionary is undefined or has zero keys
    if (!firstRowDictionary || Object.keys(firstRowDictionary).length === 0) {
      t.pass('Expected no rows to be returned');
    } else {
      t.fail('Expected no rows, but got some data');
    }
  } finally {
    await connection.close();
  }
});
