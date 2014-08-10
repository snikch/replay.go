package main

import (
	"database/sql"
	"fmt"
)

type Connection struct {
	Id           int64
	Transactions map[string]*Transaction
	Conn         *sql.DB
}

func newConnection(id int64) *Connection {
	return &Connection{
		Transactions: map[string]*Transaction{},
		Conn:         Postgres,
		Id:           id,
	}
}

func (c *Connection) ProcessQuery(query Query) error {
	// Lazily load the transaction
	transaction := c.GetTransaction(query.TransactionId)

	// Check if this is the start
	if query.TransactionBegin() {
		// Add the query to the transaction set started
		err := transaction.Start(query)
		if err != nil {
			return err
		}
		// Check if this is the end of the trnasaction
	} else if query.TransactionCommit() {
		// Error if the transaction isn't running
		if !transaction.Started {
			return fmt.Errorf("Attempted to finish transaction that isn't running (%f): %s", query.Sql, query.Score)
		}
		// Finish the transaction
		err := transaction.Add(query)
		if err != nil {
			return err
		}
		c.FlushTransaction(transaction)
		// Check if this is a rollback command
	} else if query.TransactionRollback() {
		// Remove the transaction from the queue
		transaction.Rollback()
		// Remove the transaction from the connection
		delete(c.Transactions, transaction.Id)
	} else {
		// If the transaction is running, add this to it
		if transaction.Started {
			err := transaction.Add(query)
			if err != nil {
				return err
			}
		} else {
			// Lone query
			err := transaction.Start(query)
			if err != nil {
				return err
			}
			c.FlushTransaction(transaction)
		}
	}
	return nil
}

func (c *Connection) GetTransaction(transaction_id string) *Transaction {
	transaction, ok := c.Transactions[transaction_id]
	if !ok {
		transaction = newTransaction(transaction_id)
		c.Transactions[transaction_id] = transaction
	}
	return transaction
}

// FlushTransaction adds the transaction to the global queue, and removes
// the memoized transaction from the current connection, as transaction ids
// are heavily reused.
func (c *Connection) FlushTransaction(transaction *Transaction) {
	transaction.Finish()
	c.Transactions[transaction.Id] = newTransaction(transaction.Id)
}
