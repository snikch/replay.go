package main

import "fmt"

type Connection struct {
	Transactions map[string]*Transaction
}

func newConnection() *Connection {
	return &Connection{
		Transactions: map[string]*Transaction{},
	}
}

func (c *Connection) ProcessQuery(query Query) error {
	// Lazily load the transaction
	transaction := c.GetTransaction(query.TransactionId)

	// Check if this is the start
	if query.TransactionStart() {
		// Add the query to the transaction set started
		err := transaction.Start(query)
		if err != nil {
			return err
		}
		// Check if this is the end of the trnasaction
	} else if query.TransactionEnd() {
		// Error if the transaction isn't running
		if !transaction.Started {
			return fmt.Errorf("Attempted to finish transaction that isn't running : %s", query)
		}
		// Finish the transaction
		err := transaction.Add(query)
		if err != nil {
			return err
		}
		c.FlushTransaction(transaction)
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
	primaryQueue.AddTransaction(transaction)
	delete(c.Transactions, transaction.Id)
}
