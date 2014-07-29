package main

import "sync"

var primaryQueue *Queue

func init() {
	primaryQueue = newQueue()
}

type Queue struct {
	Transactions      []*Transaction
	Queries           []Query
	TransactionsMutex sync.Mutex
	QueryMutex        sync.Mutex
}

func newQueue() *Queue {
	return &Queue{
		Transactions:      []*Transaction{},
		TransactionsMutex: sync.Mutex{},
		Queries:           []Query{},
		QueryMutex:        sync.Mutex{},
	}
}

func (q *Queue) AddTransaction(transaction *Transaction) {
	q.TransactionsMutex.Lock()
	q.Transactions = append(q.Transactions, transaction)
	q.TransactionsMutex.Unlock()
}

func (q *Queue) FlushTransactions() {
	q.TransactionsMutex.Lock()
	if len(q.Transactions) > 0 {
		if q.Transactions[0].Complete {
			transaction := q.Transactions[0]
			q.Transactions = q.Transactions[1:]
			q.QueryMutex.Lock()
			q.Queries = append(q.Queries, transaction.Queries...)
			q.QueryMutex.Unlock()
			// Does this work?
			defer q.FlushTransactions()
		}
	}
	q.TransactionsMutex.Unlock()
}
