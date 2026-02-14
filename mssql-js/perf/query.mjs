// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { openConnection, createContext } from '../__test__/db.mjs';
import Benchmark from 'benchmark';
import { Request } from '../dist/index.js';

async function querySqlServer(deferred) {
  let context = await createContext();
  let connection = await openConnection(context);
  let request = new Request(connection);
  const result = await request.query('SELECT top(4) * FROM sys.columns');
  await connection.close();
  deferred.resolve(result.rowCount);
}

const suite = new Benchmark.Suite();

// Add the async benchmark
suite
  .add('SQL Server Query', {
    defer: true,
    fn: querySqlServer,
  })
  .on('cycle', function (event) {
    console.log(String(event.target));
  })
  .on('complete', function () {
    console.log('Fastest is ' + this.filter('fastest').map('name'));
  })
  .run({ async: true });
