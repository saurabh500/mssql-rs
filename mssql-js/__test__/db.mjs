// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { create_connection } from '../dist/index.js';
import fs from 'fs';

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
        `SQL_PASSWORD environment variable not set and /tmp/password file not found: ${_err}`,
      );
    }
  }
  return password;
}

export async function nextRow(connection) {
  let metadata = await connection.internal_connection.getMetadata();

  if (!metadata) {
    return [];
  }
  let next_row = await connection.internal_connection.nextRowInResultset();
  if (!next_row) {
    if (!(await connection.internal_connection.nextResultSet())) {
      return [];
    } else {
      metadata = await connection.internal_connection.getMetadata();
      if (!metadata) {
        return [];
      }
      next_row = await connection.internal_connection.nextRowInResultset();
    }
  }
  let items = [];
  if (next_row) {
    next_row.forEach((rowVal, index) => {
      let transformed = connection.transform(metadata[index], rowVal);
      items.push(transformed);
    });
  }
  return items;
}
