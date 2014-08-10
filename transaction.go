package main

import (
	"fmt"
	"log"
)

type Transaction struct {
	Id        string
	Started   bool
	StartTime float64
	Complete  bool
	Queries   []Query
}

func newTransaction(id string) *Transaction {
	return &Transaction{
		Id:      id,
		Queries: []Query{},
	}
}

func (t *Transaction) Start(query Query) error {
	// Error if the transaction is running
	if t.Started || t.Complete {
		return fmt.Errorf("Attempted to start an already running transaction: %s", query)
	}
	t.Started = true
	t.StartTime = query.Score
	t.Queries = []Query{query}

	primaryQueue.AddTransaction(t)
	return nil
}

func (t *Transaction) Add(query Query) error {
	if !t.Started || t.Complete {
		return fmt.Errorf("Adding to a non running transaction: %s", query)
	}
	t.Queries = append(t.Queries, query)
	return nil
}

func (t *Transaction) Finish() error {
	if !t.Started {
		return fmt.Errorf("Adding to finish a non running transaction")
	}
	if t.Complete {
		return fmt.Errorf("Adding to finish an already complete transaction")
	}
	t.Complete = true
	return nil
}

func (t *Transaction) Rollback() error {
	if !t.Started {
		return fmt.Errorf("Attempting to rollback a non running transaction")
	}
	if t.Complete {
		return fmt.Errorf("Attempting to rollback an already complete transaction")
	}
	primaryQueue.RemoveTransaction(t)
	t.Started = false
	log.Printf("Removing rolled back transaction")
	return nil
}
