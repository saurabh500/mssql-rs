// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { SqlJsConnection } from '.';

export enum IsolationLevel {
  NoChange = 0x00,
  ReadUncommitted = 0x01,
  ReadCommitted = 0x02,
  RepeatableRead = 0x03,
  Serializable = 0x04,
  Snapshot = 0x05,
}

export class Transaction {
  connection: SqlJsConnection;

  constructor(connection: SqlJsConnection) {
    this.connection = connection;
  }

  /**
   * Begins a transaction with an optional isolation level. Returns a Promise that resolves to this Transaction object.
   * @param isolationLevel Optional isolation level for the transaction.
   */
  async begin(isolationLevel?: IsolationLevel): Promise<Transaction> {
    await this.connection.beginTransaction(isolationLevel);
    return this;
  }

  /**
   * Commits the transaction. Returns a Promise that resolves to this Transaction object.
   */
  async commit(): Promise<Transaction> {
    await this.connection.commitTransaction();
    return this;
  }

  /**
   * Rolls back the transaction or to a savepoint if a name is provided.
   * @param savepointName Optional savepoint name to roll back to.
   */
  async rollback(savepointName?: string): Promise<Transaction> {
    await this.connection.rollbackTransaction(savepointName);
    return this;
  }

  /**
   * Creates a savepoint with the given name.
   * @param savepointName The name of the savepoint to create.
   */
  async save(savepointName: string): Promise<Transaction> {
    await this.connection.saveTransaction(savepointName);
    return this;
  }
}
