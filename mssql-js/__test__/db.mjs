// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { create_connection } from '../dist/index.js';

export async function openConnection(context) {
  const connection = await create_connection(context);
  if (!connection) {
    throw new Error('Failed to create connection');
  }
  return connection;
}

export async function createContext() {
  const context = {
    serverName: process.env.DB_HOST || 'localhost',
    port: 1433,
    userName: process.env.DB_USER || 'sa',
    password: await getPassword(),
    database: 'master',
    trustServerCertificate: true,
  };
  return context;
}

export async function getPassword() {
  let password = process.env.SQL_PASSWORD;
  if (!password) {
    try {
      password = await fs.promises.readFile('/tmp/password', 'utf8');
      password = password.trim();
    } catch (_err) {
      throw new Error(
        'SQL_PASSWORD environment variable not set and /tmp/password file not found',
      );
    }
  }
  return password;
}
