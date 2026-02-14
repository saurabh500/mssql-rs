// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Test to verify multiple result sets work with DML + SELECT pattern
// This matches the Rust test that's currently failing

import test from 'ava';
import { createContext } from './db.mjs';
import { create_connection, Request } from '../dist/index.js';

test('multiple result sets with DML and SELECTs', async (t) => {
  try {
    const connection = await create_connection(await createContext());
    
    // This is the EXACT query pattern from the Rust test
    const query = `
      CREATE TABLE #dummy (
        IntColumn INT
      );
      INSERT INTO #dummy VALUES(10),(20);
      SELECT * FROM #dummy;
      SELECT 1;
      SELECT * FROM #dummy;
    `;
    
    let request = new Request(connection);
    let result = await request.query(query);
    
    // Log what we got
    t.log('Number of result sets:', result.IRecordSets.length);
    t.log('Result set 0 rows:', result.IRecordSets[0].length);
    t.log('Result set 1 rows:', result.IRecordSets[1].length);
    if (result.IRecordSets.length > 2) {
      t.log('Result set 2 rows:', result.IRecordSets[2].length);
      t.log('Result set 2 data:', result.IRecordSets[2]);
    }
    if (result.IRecordSets.length > 3) {
      t.log('Result set 3 rows:', result.IRecordSets[3].length);
      t.log('Result set 3 data:', result.IRecordSets[3]);
    }
    if (result.IRecordSets.length > 4) {
      t.log('Result set 4 rows:', result.IRecordSets[4].length);
      t.log('Result set 4 data:', result.IRecordSets[4]);
    }
    
    // Verify we got 3 result sets (the 3 SELECTs)
    // Note: CREATE TABLE and INSERT don't create separate result sets - they're DML operations
    // without column metadata, so they don't appear as result sets in the response
    t.is(result.IRecordSets.length, 3, 'Should have 3 result sets (3 SELECTs only)');
    
    // Result set 0: First SELECT * (2 rows)
    t.is(result.IRecordSets[0].length, 2, 'First SELECT should have 2 rows');
    t.is(result.IRecordSets[0][0].IntColumn, 10, 'First row should be 10');
    t.is(result.IRecordSets[0][1].IntColumn, 20, 'Second row should be 20');
    
    // Result set 1: SELECT 1 (1 row)
    t.is(result.IRecordSets[1].length, 1, 'SELECT 1 should have 1 row');
    // SELECT 1 returns an unnamed column
    t.is(result.IRecordSets[1][0][''], 1, 'SELECT 1 should return 1');
    
    // Result set 2: Final SELECT * (2 rows)
    t.is(result.IRecordSets[2].length, 2, 'Final SELECT should have 2 rows');
    t.is(result.IRecordSets[2][0].IntColumn, 10, 'First row should be 10');
    t.is(result.IRecordSets[2][1].IntColumn, 20, 'Second row should be 20');
    
    await connection.close();
    t.pass('Multiple result sets with DML and SELECTs work correctly');
  } catch (error) {
    t.log('Error:', error);
    t.fail('Multiple result sets test should succeed');
  }
});

test('simpler test - just SELECTs', async (t) => {
  try {
    const connection = await create_connection(await createContext());
    
    // Simpler query with just SELECTs (no DML)
    const query = 'SELECT 1, 2; SELECT 10, 20, 30;';
    
    let request = new Request(connection);
    let result = await request.query(query);
    
    t.log('Number of result sets:', result.IRecordSets.length);
    
    // Should have 2 result sets
    t.is(result.IRecordSets.length, 2, 'Should have 2 result sets');
    
    // First SELECT: 1 row with 2 columns
    t.is(result.IRecordSets[0].length, 1, 'First SELECT should have 1 row');
    
    // Second SELECT: 1 row with 3 columns
    t.is(result.IRecordSets[1].length, 1, 'Second SELECT should have 1 row');
    
    await connection.close();
    t.pass('Simple multiple SELECT test works');
  } catch (error) {
    t.log('Error:', error);
    t.fail('Simple SELECT test should succeed');
  }
});
