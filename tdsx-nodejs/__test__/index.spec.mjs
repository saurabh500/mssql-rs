import test from 'ava'

import { connect } from '../index.js'

// test('sum from native', (t) => {
//   t.is(sum(1, 2), 3)
// })

test('connect to sqlserver', async (t) => {
    // Example TypeScript test with proper typing
    const context = {
        serverName: 'localhost',
        port: 1433,
        userName: 'sa',
        password: process.env.SQL_PASSWORD,
        database: 'master',
        trustServerCertificate: true
    };
    try {
        const connection = await connect(context);
        t.pass('Connection successful');
        // Example of executing a query
        await connection.execute('SELECT * from sys.databases; ');
        let row = undefined;
        let row_count = 0;
        do {
            row = await connection.nextRow();
            if (row) {
                t.log('Row fetched:', row);
            }
            row_count++;
        } while (row.length > 0);
        t.log(`Total rows fetched: ${row_count}`);
        // await connection.close();
        t.pass('Query executed successfully');
    }
    catch (error) {
        t.log('Connection failed:', error);
        t.fail('Connection should succeed');
    }
});
