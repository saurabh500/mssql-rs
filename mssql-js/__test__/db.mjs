// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { create_connection } from '../js/index.js';

export async function openConnection(context) {
  const connection = await create_connection(context);
  if (!connection) {
    throw new Error('Failed to create connection');
  }
  return connection;
}

export function createContext() {
  const context = {
    serverName: process.env.DB_HOST || 'localhost',
    port: 1433,
    userName: process.env.DB_USER || 'sa',
    password: process.env.SQL_PASSWORD,
    database: 'master',
    trustServerCertificate: true,
  };
  return context;
}
